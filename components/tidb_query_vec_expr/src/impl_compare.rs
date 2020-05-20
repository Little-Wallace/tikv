// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::{max, Ordering};

use tidb_query_codegen::rpn_fn;

use tidb_query_common::Result;
use tidb_query_datatype::codec::collation::Collator;
use tidb_query_datatype::codec::data_type::*;

#[rpn_fn]
#[inline]
pub fn compare<C: Comparer>(lhs: &Option<C::T>, rhs: &Option<C::T>) -> Result<Option<i64>>
where
    C: Comparer,
{
    C::compare(lhs, rhs)
}

pub trait Comparer {
    type T: Evaluable;

    fn compare(lhs: &Option<Self::T>, rhs: &Option<Self::T>) -> Result<Option<i64>>;
}

pub struct BasicComparer<T: Evaluable + Ord, F: CmpOp> {
    _phantom: std::marker::PhantomData<(T, F)>,
}

impl<T: Evaluable + Ord, F: CmpOp> Comparer for BasicComparer<T, F> {
    type T = T;

    #[inline]
    fn compare(lhs: &Option<T>, rhs: &Option<T>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => Some(F::compare_order(lhs.cmp(rhs)) as i64),
        })
    }
}

pub struct StringComparer<C: Collator, F: CmpOp> {
    _phantom: std::marker::PhantomData<(C, F)>,
}

impl<C: Collator, F: CmpOp> Comparer for StringComparer<C, F> {
    type T = Bytes;

    #[inline]
    fn compare(lhs: &Option<Bytes>, rhs: &Option<Bytes>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => {
                let ord = C::sort_compare(lhs, rhs)?;
                Some(F::compare_order(ord) as i64)
            }
        })
    }
}

pub struct UintUintComparer<F: CmpOp> {
    _phantom: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for UintUintComparer<F> {
    type T = Int;

    #[inline]
    fn compare(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => {
                let lhs = *lhs as u64;
                let rhs = *rhs as u64;
                Some(F::compare_order(lhs.cmp(&rhs)) as i64)
            }
        })
    }
}

pub struct UintIntComparer<F: CmpOp> {
    _phantom: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for UintIntComparer<F> {
    type T = Int;

    #[inline]
    fn compare(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => {
                let ordering = if *rhs < 0 || *lhs as u64 > std::i64::MAX as u64 {
                    Ordering::Greater
                } else {
                    lhs.cmp(&rhs)
                };
                Some(F::compare_order(ordering) as i64)
            }
        })
    }
}

pub struct IntUintComparer<F: CmpOp> {
    _phantom: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for IntUintComparer<F> {
    type T = Int;

    #[inline]
    fn compare(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => {
                let ordering = if *lhs < 0 || *rhs as u64 > std::i64::MAX as u64 {
                    Ordering::Less
                } else {
                    lhs.cmp(&rhs)
                };
                Some(F::compare_order(ordering) as i64)
            }
        })
    }
}

pub trait CmpOp {
    #[inline]
    fn compare_null() -> Option<i64> {
        None
    }

    #[inline]
    fn compare_partial_null() -> Option<i64> {
        None
    }

    fn compare_order(ordering: std::cmp::Ordering) -> bool;
}

pub struct CmpOpLT;

impl CmpOp for CmpOpLT {
    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering == Ordering::Less
    }
}

pub struct CmpOpLE;

impl CmpOp for CmpOpLE {
    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering != Ordering::Greater
    }
}

pub struct CmpOpGT;

impl CmpOp for CmpOpGT {
    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering == Ordering::Greater
    }
}

pub struct CmpOpGE;

impl CmpOp for CmpOpGE {
    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering != Ordering::Less
    }
}

pub struct CmpOpNE;

impl CmpOp for CmpOpNE {
    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering != Ordering::Equal
    }
}

pub struct CmpOpEQ;

impl CmpOp for CmpOpEQ {
    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering == Ordering::Equal
    }
}

pub struct CmpOpNullEQ;

impl CmpOp for CmpOpNullEQ {
    #[inline]
    fn compare_null() -> Option<i64> {
        Some(1)
    }

    #[inline]
    fn compare_partial_null() -> Option<i64> {
        Some(0)
    }

    #[inline]
    fn compare_order(ordering: Ordering) -> bool {
        ordering == Ordering::Equal
    }
}

#[rpn_fn(varg)]
#[inline]
pub fn coalesce<T: Evaluable>(args: &[&Option<T>]) -> Result<Option<T>> {
    for arg in args {
        if arg.is_some() {
            return Ok((*arg).clone());
        }
    }
    Ok(None)
}

#[rpn_fn(varg, min_args = 2)]
#[inline]
pub fn greatest_int(args: &[&Option<Int>]) -> Result<Option<Int>> {
    do_get_extremum(args, max)
}

#[rpn_fn(varg, min_args = 2)]
#[inline]
pub fn greatest_real(args: &[&Option<Real>]) -> Result<Option<Real>> {
    do_get_extremum(args, |x, y| x.max(y))
}

#[inline]
fn do_get_extremum<T, E>(args: &[&Option<T>], chooser: E) -> Result<Option<T>>
where
    T: Ord + Copy,
    E: Fn(T, T) -> T,
{
    let first = args[0];
    match first {
        None => Ok(None),
        Some(first_val) => {
            let mut res = *first_val;
            for arg in &args[1..] {
                match arg {
                    None => {
                        return Ok(None);
                    }
                    Some(v) => {
                        res = chooser(res, *v);
                    }
                }
            }
            Ok(Some(res))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tidb_query_datatype::builder::FieldTypeBuilder;
    use tidb_query_datatype::{Collation, FieldTypeFlag, FieldTypeTp};
    use tipb::ScalarFuncSig;

    use crate::test_util::RpnFnScalarEvaluator;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum TestCaseCmpOp {
        GT,
        GE,
        LT,
        LE,
        EQ,
        NE,
        NullEQ,
    }

    #[allow(clippy::type_complexity)]
    fn generate_numeric_compare_cases(
    ) -> Vec<(Option<Real>, Option<Real>, TestCaseCmpOp, Option<i64>)> {
        vec![
            (None, None, TestCaseCmpOp::GT, None),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::GT, None),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::GT, None),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::GT, None),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::GT, None),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::GT,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::GT,
                Some(0),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::GT,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::GT,
                Some(0),
            ),
            (None, None, TestCaseCmpOp::GE, None),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::GE, None),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::GE, None),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::GE, None),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::GE, None),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::GE,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::GE,
                Some(0),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::GE,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::GE,
                Some(1),
            ),
            (None, None, TestCaseCmpOp::LT, None),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::LT, None),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::LT, None),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::LT, None),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::LT, None),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::LT,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::LT,
                Some(1),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::LT,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::LT,
                Some(0),
            ),
            (None, None, TestCaseCmpOp::LE, None),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::LE, None),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::LE, None),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::LE, None),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::LE, None),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::LE,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::LE,
                Some(1),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::LE,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::LE,
                Some(1),
            ),
            (None, None, TestCaseCmpOp::EQ, None),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::EQ, None),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::EQ, None),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::EQ, None),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::EQ, None),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::EQ,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::EQ,
                Some(0),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::EQ,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::EQ,
                Some(1),
            ),
            (None, None, TestCaseCmpOp::NE, None),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::NE, None),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::NE, None),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::NE, None),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::NE, None),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::NE,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::NE,
                Some(1),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::NE,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::NE,
                Some(0),
            ),
            (None, None, TestCaseCmpOp::NullEQ, Some(1)),
            (Real::new(3.5).ok(), None, TestCaseCmpOp::NullEQ, Some(0)),
            (Real::new(-2.1).ok(), None, TestCaseCmpOp::NullEQ, Some(0)),
            (None, Real::new(3.5).ok(), TestCaseCmpOp::NullEQ, Some(0)),
            (None, Real::new(-2.1).ok(), TestCaseCmpOp::NullEQ, Some(0)),
            (
                Real::new(3.5).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::NullEQ,
                Some(0),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::NullEQ,
                Some(0),
            ),
            (
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                TestCaseCmpOp::NullEQ,
                Some(1),
            ),
            (
                Real::new(-2.1).ok(),
                Real::new(-2.1).ok(),
                TestCaseCmpOp::NullEQ,
                Some(1),
            ),
        ]
    }

    #[test]
    fn test_compare_real() {
        for (arg0, arg1, cmp_op, expect_output) in generate_numeric_compare_cases() {
            let sig = match cmp_op {
                TestCaseCmpOp::GT => ScalarFuncSig::GtReal,
                TestCaseCmpOp::GE => ScalarFuncSig::GeReal,
                TestCaseCmpOp::LT => ScalarFuncSig::LtReal,
                TestCaseCmpOp::LE => ScalarFuncSig::LeReal,
                TestCaseCmpOp::EQ => ScalarFuncSig::EqReal,
                TestCaseCmpOp::NE => ScalarFuncSig::NeReal,
                TestCaseCmpOp::NullEQ => ScalarFuncSig::NullEqReal,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_compare_duration() {
        fn map_double_to_duration(v: Real) -> Duration {
            Duration::from_millis((v.into_inner() * 1000.0) as i64, 4).unwrap()
        }

        for (arg0, arg1, cmp_op, expect_output) in generate_numeric_compare_cases() {
            let sig = match cmp_op {
                TestCaseCmpOp::GT => ScalarFuncSig::GtDuration,
                TestCaseCmpOp::GE => ScalarFuncSig::GeDuration,
                TestCaseCmpOp::LT => ScalarFuncSig::LtDuration,
                TestCaseCmpOp::LE => ScalarFuncSig::LeDuration,
                TestCaseCmpOp::EQ => ScalarFuncSig::EqDuration,
                TestCaseCmpOp::NE => ScalarFuncSig::NeDuration,
                TestCaseCmpOp::NullEQ => ScalarFuncSig::NullEqDuration,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0.map(map_double_to_duration))
                .push_param(arg1.map(map_double_to_duration))
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_compare_decimal() {
        use tidb_query_datatype::codec::convert::ConvertTo;
        use tidb_query_datatype::expr::EvalContext;
        fn f64_to_decimal(ctx: &mut EvalContext, f: f64) -> Result<Decimal> {
            let val = f.convert(ctx)?;
            Ok(val)
        }
        let mut ctx = EvalContext::default();
        for (arg0, arg1, cmp_op, expect_output) in generate_numeric_compare_cases() {
            let sig = match cmp_op {
                TestCaseCmpOp::GT => ScalarFuncSig::GtDecimal,
                TestCaseCmpOp::GE => ScalarFuncSig::GeDecimal,
                TestCaseCmpOp::LT => ScalarFuncSig::LtDecimal,
                TestCaseCmpOp::LE => ScalarFuncSig::LeDecimal,
                TestCaseCmpOp::EQ => ScalarFuncSig::EqDecimal,
                TestCaseCmpOp::NE => ScalarFuncSig::NeDecimal,
                TestCaseCmpOp::NullEQ => ScalarFuncSig::NullEqDecimal,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0.map(|v| f64_to_decimal(&mut ctx, v.into_inner()).unwrap()))
                .push_param(arg1.map(|v| f64_to_decimal(&mut ctx, v.into_inner()).unwrap()))
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_compare_signed_int() {
        for (arg0, arg1, cmp_op, expect_output) in generate_numeric_compare_cases() {
            let sig = match cmp_op {
                TestCaseCmpOp::GT => ScalarFuncSig::GtInt,
                TestCaseCmpOp::GE => ScalarFuncSig::GeInt,
                TestCaseCmpOp::LT => ScalarFuncSig::LtInt,
                TestCaseCmpOp::LE => ScalarFuncSig::LeInt,
                TestCaseCmpOp::EQ => ScalarFuncSig::EqInt,
                TestCaseCmpOp::NE => ScalarFuncSig::NeInt,
                TestCaseCmpOp::NullEQ => ScalarFuncSig::NullEqInt,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0.map(|v| v.into_inner() as i64))
                .push_param(arg1.map(|v| v.into_inner() as i64))
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_compare_int_2() {
        let test_cases = vec![
            (Some(5), false, Some(3), false, Ordering::Greater),
            (
                Some(std::u64::MAX as i64),
                false,
                Some(5),
                false,
                Ordering::Less,
            ),
            (
                Some(std::u64::MAX as i64),
                true,
                Some((std::u64::MAX - 1) as i64),
                true,
                Ordering::Greater,
            ),
            (
                Some(std::u64::MAX as i64),
                true,
                Some(5),
                true,
                Ordering::Greater,
            ),
            (Some(5), true, Some(std::i64::MIN), false, Ordering::Greater),
            (
                Some(std::u64::MAX as i64),
                true,
                Some(std::i64::MIN),
                false,
                Ordering::Greater,
            ),
            (Some(5), true, Some(3), false, Ordering::Greater),
            (Some(std::i64::MIN), false, Some(3), true, Ordering::Less),
            (
                Some(5),
                false,
                Some(std::u64::MAX as i64),
                true,
                Ordering::Less,
            ),
            (Some(5), false, Some(3), true, Ordering::Greater),
        ];
        for (lhs, lhs_is_unsigned, rhs, rhs_is_unsigned, ordering) in test_cases {
            let lhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if lhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();
            let rhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if rhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();

            for (sig, accept_orderings) in &[
                (ScalarFuncSig::EqInt, vec![Ordering::Equal]),
                (
                    ScalarFuncSig::NeInt,
                    vec![Ordering::Greater, Ordering::Less],
                ),
                (ScalarFuncSig::GtInt, vec![Ordering::Greater]),
                (
                    ScalarFuncSig::GeInt,
                    vec![Ordering::Greater, Ordering::Equal],
                ),
                (ScalarFuncSig::LtInt, vec![Ordering::Less]),
                (ScalarFuncSig::LeInt, vec![Ordering::Less, Ordering::Equal]),
            ] {
                let output = RpnFnScalarEvaluator::new()
                    .push_param_with_field_type(lhs, lhs_field_type.clone())
                    .push_param_with_field_type(rhs, rhs_field_type.clone())
                    .evaluate(*sig)
                    .unwrap();
                if accept_orderings.iter().any(|&x| x == ordering) {
                    assert_eq!(output, Some(1));
                } else {
                    assert_eq!(output, Some(0));
                }
            }
        }
    }

    #[test]
    fn test_compare_string() {
        fn should_match(ord: Ordering, sig: ScalarFuncSig) -> bool {
            match ord {
                Ordering::Less => {
                    sig == ScalarFuncSig::LtString
                        || sig == ScalarFuncSig::LeString
                        || sig == ScalarFuncSig::NeString
                }
                Ordering::Equal => {
                    sig == ScalarFuncSig::EqString
                        || sig == ScalarFuncSig::LeString
                        || sig == ScalarFuncSig::GeString
                }
                Ordering::Greater => {
                    sig == ScalarFuncSig::GtString
                        || sig == ScalarFuncSig::GeString
                        || sig == ScalarFuncSig::NeString
                }
            }
        }

        let signatures = vec![
            ScalarFuncSig::LtString,
            ScalarFuncSig::LeString,
            ScalarFuncSig::GtString,
            ScalarFuncSig::GeString,
            ScalarFuncSig::EqString,
            ScalarFuncSig::NeString,
        ];
        let cases = vec![
            // strA, strB, [binOrd, utfbin_no_padding, utf8bin, ciOrd]
            (
                "",
                " ",
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Equal,
                ],
            ),
            (
                "a",
                "b",
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                ],
            ),
            (
                "a",
                "A",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Equal,
                ],
            ),
            (
                "a",
                "A ",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Equal,
                ],
            ),
            (
                "a",
                "a ",
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Equal,
                ],
            ),
            (
                "À",
                "A",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Equal,
                ],
            ),
            (
                "À\t",
                "A",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                ],
            ),
            (
                "abc",
                "ab",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                ],
            ),
            (
                "a bc",
                "ab ",
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                ],
            ),
            (
                "Abc",
                "abC",
                [
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Less,
                    Ordering::Equal,
                ],
            ),
            (
                "filé-110",
                "file-12",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Less,
                ],
            ),
            (
                "😜",
                "😃",
                [
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Greater,
                    Ordering::Equal,
                ],
            ),
        ];
        let collations = [
            (Collation::Binary, 0),
            (Collation::Utf8Mb4BinNoPadding, 1),
            (Collation::Utf8Mb4Bin, 2),
            (Collation::Utf8Mb4GeneralCi, 3),
        ];

        for (str_a, str_b, ordering_in_collations) in cases {
            for &sig in &signatures {
                for &(collation, index) in &collations {
                    let result: i64 = RpnFnScalarEvaluator::new()
                        .push_param(str_a.as_bytes().to_vec())
                        .push_param(str_b.as_bytes().to_vec())
                        .return_field_type(
                            FieldTypeBuilder::new()
                                .tp(FieldTypeTp::Long)
                                .collation(collation),
                        )
                        .evaluate(sig)
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        should_match(ordering_in_collations[index], sig) as i64,
                        result,
                        "Unexpected {:?}({}, {}) == {} in {}",
                        sig,
                        str_a,
                        str_b,
                        result,
                        collation
                    );
                }
            }
        }
    }

    #[test]
    fn test_coalesce() {
        let cases = vec![
            (vec![], None),
            (vec![None], None),
            (vec![None, None], None),
            (vec![None, None, None], None),
            (vec![None, Some(0), None], Some(0)),
        ];
        for (args, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate(ScalarFuncSig::CoalesceInt)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_greatest_int() {
        let cases = vec![
            (vec![None, None], None),
            (vec![Some(1), Some(1)], Some(1)),
            (vec![Some(1), Some(-1), None], None),
            (vec![Some(-2), Some(-1), Some(1), Some(2)], Some(2)),
            (
                vec![Some(i64::MIN), Some(0), Some(-1), Some(i64::MAX)],
                Some(i64::MAX),
            ),
            (vec![Some(0), Some(4), Some(8), Some(8)], Some(8)),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::GreatestInt)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_greatest_real() {
        let cases = vec![
            (vec![None, None], None),
            (vec![Real::new(1.0).ok(), Real::new(-1.0).ok(), None], None),
            (
                vec![
                    Real::new(1.0).ok(),
                    Real::new(-1.0).ok(),
                    Real::new(-2.0).ok(),
                    Real::new(0f64).ok(),
                ],
                Real::new(1.0).ok(),
            ),
            (
                vec![
                    Real::new(f64::MAX).ok(),
                    Real::new(f64::MIN).ok(),
                    Real::new(0f64).ok(),
                ],
                Real::new(f64::MAX).ok(),
            ),
            (vec![Real::new(f64::NAN).ok(), Real::new(0f64).ok()], None),
            (
                vec![
                    Real::new(f64::INFINITY).ok(),
                    Real::new(f64::NEG_INFINITY).ok(),
                    Real::new(f64::MAX).ok(),
                    Real::new(f64::MIN).ok(),
                ],
                Real::new(f64::INFINITY).ok(),
            ),
        ];

        for (row, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::GreatestReal)
                .unwrap();
            assert_eq!(output, expected);
        }
    }
}