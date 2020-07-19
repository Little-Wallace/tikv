// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This is the core implementation of a batch system. Generally there will be two
//! different kind of FSMs in TiKV's FSM system. One is normal FSM, which usually
//! represents a peer, the other is control FSM, which usually represents something
//! that controls how the former is created or metrics are collected.

use crate::config::Config;
use crate::fsm::{Fsm, FsmScheduler};
use crate::mailbox::BasicMailbox;
use crate::router::Router;
use crossbeam::channel::{self, SendError};
use std::borrow::Cow;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tikv_util::mpsc;

/// A unify type for FSMs so that they can be sent to channel easily.
enum FsmTypes<N, C> {
    Normal(Box<N>),
    Control(Box<C>),
    // Used as a signal that scheduler should be shutdown.
    Empty,
}

// A macro to introduce common definition of scheduler.
macro_rules! impl_sched {
    ($name:ident, $ty:path, Fsm = $fsm:tt) => {
        pub struct $name<N, C> {
            sender: channel::Sender<FsmTypes<N, C>>,
        }

        impl<N, C> Clone for $name<N, C> {
            #[inline]
            fn clone(&self) -> $name<N, C> {
                $name {
                    sender: self.sender.clone(),
                }
            }
        }

        impl<N, C> FsmScheduler for $name<N, C>
        where
            $fsm: Fsm,
        {
            type Fsm = $fsm;

            #[inline]
            fn schedule(&self, fsm: Box<Self::Fsm>) {
                match self.sender.send($ty(fsm)) {
                    Ok(()) => {}
                    // TODO: use debug instead.
                    Err(SendError($ty(fsm))) => warn!("failed to schedule fsm {:p}", fsm),
                    _ => unreachable!(),
                }
            }

            fn shutdown(&self) {
                // TODO: close it explicitly once it's supported.
                // Magic number, actually any number greater than poll pool size works.
                for _ in 0..100 {
                    let _ = self.sender.send(FsmTypes::Empty);
                }
            }
        }
    };
}

impl_sched!(NormalScheduler, FsmTypes::Normal, Fsm = N);
impl_sched!(ControlScheduler, FsmTypes::Control, Fsm = C);

/// A basic struct for a round of polling.
#[allow(clippy::vec_box)]
pub struct BatchContext<N: Fsm, C: Fsm> {
    normals: Vec<Box<N>>,
    control: Option<Box<C>>,
    fsm_receiver: channel::Receiver<FsmTypes<N, C>>,
    router: Router<N, C, NormalScheduler<N, C>, ControlScheduler<N, C>>,
    reschedule_duration: Duration,
    run: bool,
}

impl<N: Fsm, C: Fsm> BatchContext<N, C> {
    pub fn push(&mut self, fsm: Box<N>) {
        self.normals.push(fsm);
    }

    pub fn get_mut(&mut self) -> &mut [Box<N>] {
        &mut self.normals
    }

    pub fn release(&mut self, reschedule: bool) {
        if self.normals.is_empty() {
            return;
        }
        let batch = std::mem::replace(&mut self.normals, vec![]);
        for p in batch.into_iter() {
            if p.is_stopped() {
                self.remove(p);
            } else {
                self.release_normal(p, reschedule);
            }
        }
    }

    /// Put back the FSM located at index.
    ///
    /// Only when channel length is larger than `checked_len` will trigger
    /// further notification. This function may fail if channel length is
    /// larger than the given value before FSM is released.
    pub fn release_normal(&mut self, mut fsm: Box<N>, reschedule: bool) {
        let need_reschedule = reschedule || fsm.schedule_time() > self.reschedule_duration;
        let check_len = match fsm.check_len() {
            Some(l) => l,
            None => {
                if need_reschedule {
                    self.router.normal_scheduler.schedule(fsm);
                } else {
                    self.normals.push(fsm);
                }
                return;
            }
        };
        let mailbox = fsm.take_mailbox().unwrap();
        mailbox.release(fsm);
        if mailbox.len() != check_len {
            match mailbox.take_fsm() {
                None => (),
                Some(mut s) => {
                    s.set_mailbox(Cow::Owned(mailbox));
                    if need_reschedule {
                        self.router.normal_scheduler.schedule(s);
                    } else {
                        self.normals.push(s);
                    }
                }
            }
        }
    }

    /// Remove the normal FSM located at `index`.
    ///
    /// This method should only be called when the FSM is stopped.
    /// If there are still messages in channel, the FSM is untouched and
    /// the function will return false to let caller to keep polling.
    fn remove(&mut self, mut fsm: Box<N>) {
        let mailbox = fsm.take_mailbox().unwrap();
        if mailbox.is_empty() {
            mailbox.release(fsm);
        } else {
            fsm.set_mailbox(Cow::Owned(mailbox));
            self.normals.push(fsm);
        }
    }

    /// Same as `release`, but working on control FSM.
    fn release_control(&mut self, fsm: Box<C>) {
        let check_len = match fsm.check_len() {
            Some(l) => l,
            None => {
                self.control = Some(fsm);
                return;
            }
        };
        self.router.control_box.release(fsm);
        if self.router.control_box.len() != check_len {
            match self.router.control_box.take_fsm() {
                Some(s) => {
                    self.control = Some(s);
                }
                None => (),
            }
        }
    }

    /// Same as `remove`, but working on control FSM.
    fn remove_control(&mut self, fsm: Box<C>) {
        if self.router.control_box.is_empty() {
            self.router.control_box.release(fsm);
        }
    }

    fn transfer(&mut self, fsm: FsmTypes<N, C>) -> Option<Box<N>> {
        match fsm {
            FsmTypes::Normal(p) => Some(p),
            FsmTypes::Control(p) => {
                self.control = Some(p);
                None
            }
            FsmTypes::Empty => {
                self.run = false;
                None
            }
        }
    }

    fn try_recv(&mut self) -> Option<Box<N>> {
        self.fsm_receiver
            .try_recv()
            .ok()
            .and_then(|fsm| self.transfer(fsm))
    }
    fn recv(&mut self) -> Option<Box<N>> {
        self.fsm_receiver
            .recv()
            .ok()
            .and_then(|fsm| self.transfer(fsm))
    }

    fn is_empty(&self) -> bool {
        self.normals.is_empty() && self.control.is_none()
    }

    fn clear(&mut self) {
        self.normals.clear();
        self.control.take();
    }
}

/// A handler that poll all FSM in ready.
///
/// A General process works like following:
/// ```text
/// loop {
///     begin
///     if control is ready:
///         handle_control
///     foreach ready normal:
///         handle_normal
///     end
/// }
/// ```
///
/// Note that, every poll thread has its own handler, which doesn't have to be
/// Sync.
pub trait PollHandler<N: Fsm, C: Fsm> {
    /// This function is called at the very beginning of every round.
    fn begin(&mut self, batch_size: usize);

    /// This function is called when handling readiness for control FSM.
    ///
    /// If returned value is Some, then it represents a length of channel. This
    /// function will only be called for the same fsm after channel's lengh is
    /// larger than the value. If it returns None, then this function will
    /// still be called for the same FSM in the next loop unless the FSM is
    /// stopped.
    fn handle_control(&mut self, control: &mut C);

    /// This function is called when handling readiness for normal FSM.
    ///
    /// The returned value is handled in the same way as `handle_control`.
    fn handle_normal(&mut self, ctx: &mut BatchContext<N, C>, normal: Box<N>);

    /// This function is called at the end of every round.
    fn end(&mut self, ctx: &mut BatchContext<N, C>);

    /// This function is called when batch system is going to sleep.
    fn pause(&mut self) {}
}

/// Internal poller that fetches batch and call handler hooks for readiness.
struct Poller<N: Fsm, C: Fsm, Handler> {
    ctx: BatchContext<N, C>,
    handler: Handler,
    max_batch_size: usize,
}

impl<N: Fsm, C: Fsm, Handler: PollHandler<N, C>> Poller<N, C, Handler> {
    // Poll for readiness and forward to handler. Remove stale peer if necessary.
    fn poll(&mut self) {
        // Fetch batch after every round is finished. It's helpful to protect regions
        // from becoming hungry if some regions are hot points. Since we fetch new fsm every time
        // calling `poll`, we do not need to configure a large value for `self.max_batch_size`.
        while self.ctx.run {
            if self.ctx.is_empty() {
                self.handler.pause();
                if let Some(fsm) = self.ctx.recv() {
                    self.ctx.push(fsm);
                } else if !self.ctx.run {
                    break;
                }
            }
            self.handler.begin(self.max_batch_size);
            if let Some(mut fsm) = self.ctx.control.take() {
                self.handler.handle_control(&mut fsm);
                if fsm.is_stopped() {
                    self.ctx.remove_control(fsm);
                } else {
                    self.ctx.release_control(fsm);
                }
            }

            let mut fsm_cnt = self.ctx.normals.len();
            if !self.ctx.normals.is_empty() {
                let normals = std::mem::replace(&mut self.ctx.normals, vec![]);
                for p in normals.into_iter() {
                    self.handler.handle_normal(&mut self.ctx, p);
                }
            }

            while fsm_cnt < self.max_batch_size {
                if let Some(fsm) = self.ctx.try_recv() {
                    self.handler.handle_normal(&mut self.ctx, fsm);
                    fsm_cnt += 1;
                } else {
                    break;
                }
            }
            self.handler.end(&mut self.ctx);
            self.ctx.release(false);
        }
        self.ctx.clear();
    }
}

/// A builder trait that can build up poll handlers.
pub trait HandlerBuilder<N: Fsm, C: Fsm> {
    type Handler: PollHandler<N, C>;

    fn build(&mut self) -> Self::Handler;
}

/// A system that can poll FSMs concurrently and in batch.
///
/// To use the system, two type of FSMs and their PollHandlers need
/// to be defined: Normal and Control. Normal FSM handles the general
/// task while Control FSM creates normal FSM instances.
pub struct BatchSystem<N: Fsm, C: Fsm> {
    name_prefix: Option<String>,
    router: BatchRouter<N, C>,
    receiver: channel::Receiver<FsmTypes<N, C>>,
    pool_size: usize,
    max_batch_size: usize,
    workers: Vec<JoinHandle<()>>,
    reschedule_duration: Duration,
}

impl<N, C> BatchSystem<N, C>
where
    N: Fsm + Send + 'static,
    C: Fsm + Send + 'static,
{
    pub fn router(&self) -> &BatchRouter<N, C> {
        &self.router
    }

    /// Start the batch system.
    pub fn spawn<B>(&mut self, name_prefix: String, mut builder: B)
    where
        B: HandlerBuilder<N, C>,
        B::Handler: Send + 'static,
    {
        for i in 0..self.pool_size {
            let handler = builder.build();
            let ctx = BatchContext {
                router: self.router.clone(),
                fsm_receiver: self.receiver.clone(),
                reschedule_duration: self.reschedule_duration,
                normals: vec![],
                control: None,
                run: true,
            };
            let mut poller = Poller {
                ctx,
                handler,
                max_batch_size: self.max_batch_size,
            };
            let t = thread::Builder::new()
                .name(thd_name!(format!("{}-{}", name_prefix, i)))
                .spawn(move || poller.poll())
                .unwrap();
            self.workers.push(t);
        }
        self.name_prefix = Some(name_prefix);
    }

    /// Shutdown the batch system and wait till all background threads exit.
    pub fn shutdown(&mut self) {
        if self.name_prefix.is_none() {
            return;
        }
        let name_prefix = self.name_prefix.take().unwrap();
        info!("shutdown batch system {}", name_prefix);
        self.router.broadcast_shutdown();
        let mut last_error = None;
        for h in self.workers.drain(..) {
            debug!("waiting for {}", h.thread().name().unwrap());
            if let Err(e) = h.join() {
                error!("failed to join worker thread: {:?}", e);
                last_error = Some(e);
            }
        }
        if let Some(e) = last_error {
            if !thread::panicking() {
                panic!("failed to join worker thread: {:?}", e);
            }
        }
        info!("batch system {} is stopped.", name_prefix);
    }
}

pub type BatchRouter<N, C> = Router<N, C, NormalScheduler<N, C>, ControlScheduler<N, C>>;

/// Create a batch system with the given thread name prefix and pool size.
///
/// `sender` and `controller` should be paired.
pub fn create_system<N: Fsm, C: Fsm>(
    cfg: &Config,
    sender: mpsc::LooseBoundedSender<C::Message>,
    controller: Box<C>,
) -> (BatchRouter<N, C>, BatchSystem<N, C>) {
    let control_box = BasicMailbox::new(sender, controller);
    let (tx, rx) = channel::unbounded();
    let normal_scheduler = NormalScheduler { sender: tx.clone() };
    let control_scheduler = ControlScheduler { sender: tx };
    let router = Router::new(control_box, normal_scheduler, control_scheduler);
    let system = BatchSystem {
        name_prefix: None,
        router: router.clone(),
        receiver: rx,
        pool_size: cfg.pool_size,
        max_batch_size: cfg.max_batch_size,
        reschedule_duration: cfg.reschedule_duration.0,
        workers: vec![],
    };
    (router, system)
}
