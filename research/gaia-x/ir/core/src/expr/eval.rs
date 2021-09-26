//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.
//!

use crate::error::{ParsePbError, ParsePbResult};
use crate::expr::error::{ExprError, ExprResult};
use crate::generated::common as pb;
use crate::graph::element::Element;
use crate::graph::property::{Details, PropKey};
use crate::{FromPb, NameOrId};
use dyn_type::arith::Exp;
use dyn_type::{BorrowObject, Object};
use std::cell::RefCell;

pub struct Evaluator<'a> {
    /// A suffix-tree-based expression for evaluating
    suffix_tree: Vec<InnerOpr>,
    /// A stack for evaluating the suffix-tree-based expression
    /// Wrap it in a `RefCell` to avoid conflict mutable reference
    stack: RefCell<Vec<BorrowObject<'a>>>,
}

/// An inner representation of `pb::ExprOpr` for one-shot translation of `pb::ExprOpr`.
#[derive(Debug, Clone)]
pub(crate) enum InnerOpr {
    Logical(pb::Logical),
    Arith(pb::Arithmetic),
    Const(Option<Object>),
    Var {
        tag: NameOrId,
        prop_key: Option<PropKey>,
    },
}

impl ToString for InnerOpr {
    fn to_string(&self) -> String {
        match self {
            InnerOpr::Logical(logical) => format!("{:?}", logical),
            InnerOpr::Arith(arith) => format!("{:?}", arith),
            InnerOpr::Const(c) => format!("Const({:?})", c),
            InnerOpr::Var { tag, prop_key } => {
                if let Some(p) = prop_key {
                    format!("Var (tag: {:?}, prop_key: {:?})", tag, p)
                } else {
                    format!("Var (tag: {:?})", tag)
                }
            }
        }
    }
}

/// A string representation of `InnerOpr`
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorDesc(String);

impl From<InnerOpr> for OperatorDesc {
    fn from(inner: InnerOpr) -> Self {
        Self::from(&inner)
    }
}

impl From<&InnerOpr> for OperatorDesc {
    fn from(inner: &InnerOpr) -> Self {
        Self(inner.to_string())
    }
}

/// A `Context` gives the behavior of obtaining a certain tag from the runtime
/// for evaluating variables in an expression.
pub trait Context<E: Element> {
    fn get(&self, _tag: &NameOrId) -> Option<&E> {
        None
    }
}

pub struct NoneContext {}

impl Context<()> for NoneContext {}

impl<'a> FromPb<pb::SuffixExpr> for Evaluator<'a> {
    fn from_pb(suffix_tree: pb::SuffixExpr) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        let mut inner_tree: Vec<InnerOpr> = Vec::with_capacity(suffix_tree.operators.len());
        for unit in suffix_tree.operators {
            inner_tree.push(InnerOpr::from_pb(unit)?);
        }
        Ok(Self {
            suffix_tree: inner_tree,
            stack: RefCell::new(vec![]),
        })
    }
}

fn apply_arith<'a>(
    arith: &pb::Arithmetic,
    first: Option<BorrowObject<'a>>,
    second: Option<BorrowObject<'a>>,
) -> ExprResult<BorrowObject<'a>> {
    use pb::Arithmetic::*;
    if first.is_some() && second.is_some() {
        let a = first.unwrap();
        let b = second.unwrap();
        Ok(match arith {
            Add => BorrowObject::Primitive(a.as_primitive()? + b.as_primitive()?),
            Sub => BorrowObject::Primitive(a.as_primitive()? - b.as_primitive()?),
            Mul => BorrowObject::Primitive(a.as_primitive()? * b.as_primitive()?),
            Div => BorrowObject::Primitive(a.as_primitive()? / b.as_primitive()?),
            Mod => BorrowObject::Primitive(a.as_primitive()? % b.as_primitive()?),
            Exp => BorrowObject::Primitive(a.as_primitive()?.exp(b.as_primitive()?)),
        })
    } else {
        Err(ExprError::MissingOperands(InnerOpr::Arith(*arith).into()))
    }
}

fn apply_logical<'a>(
    logical: &pb::Logical,
    first: Option<BorrowObject<'a>>,
    second: Option<BorrowObject<'a>>,
) -> ExprResult<BorrowObject<'a>> {
    use pb::Logical::*;
    if logical == &Not {
        if let Some(a) = first {
            return Ok((!a.as_bool()?).into());
        }
    } else {
        if first.is_some() && second.is_some() {
            let a = first.unwrap();
            let b = second.unwrap();
            let rst = match logical {
                Eq => Ok((a == b).into()),
                Ne => Ok((a != b).into()),
                Lt => Ok((a < b).into()),
                Le => Ok((a <= b).into()),
                Gt => Ok((a > b).into()),
                Ge => Ok((a >= b).into()),
                And => Ok((a.as_bool()? && b.as_bool()?).into()),
                Or => Ok((a.as_bool()? || b.as_bool()?).into()),
                Not => unreachable!(),
                // todo within, without
                _ => Err(ExprError::OtherErr(
                    "`within`, `without` unimplemented!".to_string(),
                )),
            };
            return rst;
        }
    }

    Err(ExprError::MissingOperands(
        InnerOpr::Logical(*logical).into(),
    ))
}

// Private api
impl<'a> Evaluator<'a> {
    /// Evaluate simple expression that contains less than three operators
    /// without using the stack.
    fn eval_without_stack<E: Element, C: Context<E>>(
        &'a self,
        context: Option<&C>,
    ) -> ExprResult<Object> {
        assert!(self.suffix_tree.len() <= 3);
        let _first = self.suffix_tree.get(0);
        let _second = self.suffix_tree.get(1);
        let _third = self.suffix_tree.get(2);
        if self.suffix_tree.is_empty() {
            Err(ExprError::EmptyExpression)
        } else if self.suffix_tree.len() == 1 {
            let first = _first.unwrap();
            Ok(first
                .eval_as_borrow_object(context)?
                .ok_or(ExprError::NoneOperand(first.into()))?
                .into())
        } else if self.suffix_tree.len() == 2 {
            let first = _first.unwrap();
            let second = _second.unwrap();
            // must be not
            if let InnerOpr::Logical(logical) = second {
                Ok(apply_logical(logical, first.eval_as_borrow_object(context)?, None)?.into())
            } else {
                if !second.is_operand() {
                    Err(ExprError::MissingOperands(second.into()))
                } else {
                    Err(ExprError::OtherErr("invalid expression".to_string()))
                }
            }
        } else {
            let first = _first.unwrap();
            let second = _second.unwrap();
            let third = _third.unwrap();

            if let InnerOpr::Logical(logical) = third {
                Ok(apply_logical(
                    logical,
                    first.eval_as_borrow_object(context)?,
                    second.eval_as_borrow_object(context)?,
                )?
                .into())
            } else if let InnerOpr::Arith(arith) = third {
                Ok(apply_arith(
                    arith,
                    first.eval_as_borrow_object(context)?,
                    second.eval_as_borrow_object(context)?,
                )?
                .into())
            } else {
                Err(ExprError::OtherErr("invalid expression".to_string()))
            }
        }
    }
}

impl<'a> Evaluator<'a> {
    /// Reset the status of the evaluator for further evaluation
    pub fn reset(&self) {
        self.stack.borrow_mut().clear();
    }

    /// Evaluate an expression with an optional context. The context must implement the
    /// provided trait `[Context]`, that can get an `[crate::graph::element::Element]`
    /// using a given key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use ir_core::graph::element::Vertex;
    /// # use ir_core::expr::eval::{Context, Evaluator};
    /// # use ir_core::{NameOrId, FromPb};
    /// # use ir_core::graph::property::{DefaultDetails, DynDetails, Label};
    /// # use std::collections::HashMap;
    /// # use dyn_type::Object;
    /// # use ir_core::expr::token::tokenize;
    /// # use ir_core::expr::to_suffix_expr_pb;
    ///
    /// struct Vertices {
    ///     vec: Vec<Vertex>,
    /// }
    ///
    /// impl Context<Vertex> for Vertices {
    ///     fn get(&self, key: &NameOrId) -> Option<&Vertex> {
    ///        match key {
    ///             NameOrId::Str(_) => None,
    ///             NameOrId::Id(i) => self.vec.get(*i as usize),
    ///         }
    ///     }
    /// }
    ///
    /// let map: HashMap<NameOrId, Object> = vec![
    ///         (NameOrId::from("age".to_string()), 12.into()),
    ///         (NameOrId::from("birthday".to_string()), 19900416.into()),
    ///     ]
    ///     .into_iter()
    ///     .collect();
    ///
    ///     let ctxt = Vertices {
    ///         vec: vec![
    ///             Vertex::new(DynDetails::new(DefaultDetails::with_property(
    ///                 1,
    ///                 Label::from(1),
    ///                 map.clone(),
    ///             ))),
    ///             Vertex::new(DynDetails::new(DefaultDetails::with_property(
    ///                 2,
    ///                 Label::from(2),
    ///                 map.clone(),
    ///             ))),
    ///         ],
    ///     };
    ///
    /// let tokens = tokenize("@0.age == @1.age").unwrap();
    /// let suffix_tree = to_suffix_expr_pb(tokens).unwrap();
    /// let eval = Evaluator::from_pb(suffix_tree).unwrap();
    ///
    /// assert!(eval.eval::<_, _>(Some(&ctxt)).unwrap().as_bool().unwrap())
    /// ```
    pub fn eval<E: Element + 'a, C: Context<E> + 'a>(
        &'a self,
        context: Option<&'a C>,
    ) -> ExprResult<Object> {
        let mut stack = self.stack.borrow_mut();
        if self.suffix_tree.len() <= 3 {
            return self.eval_without_stack(context);
        }
        stack.clear();
        for opr in &self.suffix_tree {
            if opr.is_operand() {
                if let Some(obj) = opr.eval_as_borrow_object(context)? {
                    stack.push(obj);
                } else {
                    return Err(ExprError::NoneOperand(opr.into()));
                }
            } else {
                let first = stack.pop();
                match opr {
                    InnerOpr::Logical(logical) => {
                        let rst = if logical == &pb::Logical::Not {
                            apply_logical(logical, first, None)?
                        } else {
                            apply_logical(logical, stack.pop(), first)?
                        };
                        stack.push(rst);
                    }
                    InnerOpr::Arith(arith) => {
                        let rst = apply_arith(arith, stack.pop(), first)?;
                        stack.push(rst);
                    }
                    _ => unreachable!(),
                }
            }
        }

        if stack.len() == 1 {
            Ok(stack.pop().unwrap().into())
        } else {
            Err("invalid expression".into())
        }
    }

    pub fn eval_bool<E: Element + 'a, C: Context<E> + 'a>(
        &'a self,
        context: Option<&'a C>,
    ) -> ExprResult<bool> {
        Ok(self.eval(context)?.as_bool()?)
    }
}

impl FromPb<pb::ExprOpr> for InnerOpr {
    fn from_pb(unit: pb::ExprOpr) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        use pb::expr_opr::Item::*;
        if let Some(item) = unit.item {
            let result = match item {
                Logical(logical) => {
                    Self::Logical(unsafe { std::mem::transmute::<_, pb::Logical>(logical) })
                }
                Arith(arith) => {
                    Self::Arith(unsafe { std::mem::transmute::<_, pb::Arithmetic>(arith) })
                }
                Const(c) => Self::Const(c.into_object()?),
                Var(var) => {
                    let tag = NameOrId::from_pb(var.tag.unwrap())?;
                    if let Some(property) = var.property {
                        Self::Var {
                            tag,
                            prop_key: Some(PropKey::from_pb(property)?),
                        }
                    } else {
                        Self::Var {
                            tag,
                            prop_key: None,
                        }
                    }
                }
            };
            Ok(result)
        } else {
            Err(ParsePbError::from("empty value provided"))
        }
    }
}

impl InnerOpr {
    pub fn eval_as_borrow_object<'a, E: Element + 'a, C: Context<E> + 'a>(
        &'a self,
        context: Option<&'a C>,
    ) -> ExprResult<Option<BorrowObject<'a>>> {
        match self {
            Self::Const(c_opt) => Ok(if let Some(opt) = c_opt {
                Some(opt.as_borrow())
            } else {
                None
            }),
            Self::Var { tag, prop_key } => {
                if let Some(ctxt) = context {
                    let mut result = Ok(None);
                    if let Some(element) = ctxt.get(tag) {
                        if let Some(property) = prop_key {
                            if let Some(details) = element.details() {
                                result = Ok(details.get(property))
                            }
                        } else {
                            result = Ok(Some(element.as_borrow_object()))
                        }
                    }

                    result
                } else {
                    Err(ExprError::MissingContext(self.into()))
                }
            }
            _ => Err(ExprError::UnmatchedOperator(self.into())),
        }
    }

    pub fn is_operand(&self) -> bool {
        match self {
            InnerOpr::Const(_) | InnerOpr::Var { .. } => true,
            _ => false,
        }
    }
}

impl pb::Const {
    pub fn into_object(self) -> ParsePbResult<Option<Object>> {
        use pb::value::Item::*;
        if let Some(val) = &self.value {
            if let Some(item) = val.item.as_ref() {
                return match item {
                    Boolean(b) => Ok(Some((*b).into())),
                    I32(i) => Ok(Some((*i).into())),
                    I64(i) => Ok(Some((*i).into())),
                    F64(f) => Ok(Some((*f).into())),
                    Str(s) => Ok(Some(s.clone().into())),
                    Blob(blob) => Ok(Some(blob.clone().into())),
                    None(_) => Ok(Option::None),
                    I32Array(_) | I64Array(_) | F64Array(_) | StrArray(_) => {
                        Err(ParsePbError::from("the const values of `I32Array`, `I64Array`, `F64Array`, `StrArray` are unsupported"))
                    }
                };
            }
        }

        Err(ParsePbError::from("empty value provided"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::to_suffix_expr_pb;
    use crate::expr::token::tokenize;
    use crate::graph::element::Vertex;
    use crate::graph::property::{DefaultDetails, DynDetails, Label};
    use std::collections::HashMap;

    struct Vertices {
        vec: Vec<Vertex>,
    }
    impl Context<Vertex> for Vertices {
        fn get(&self, key: &NameOrId) -> Option<&Vertex> {
            match key {
                NameOrId::Str(_) => None,
                NameOrId::Id(i) => self.vec.get(*i as usize),
            }
        }
    }

    fn prepare_context() -> Vertices {
        let map1: HashMap<NameOrId, Object> = vec![
            (NameOrId::from("age".to_string()), 31.into()),
            (NameOrId::from("birthday".to_string()), 19900416.into()),
            (
                NameOrId::from("name".to_string()),
                "John".to_string().into(),
            ),
        ]
        .into_iter()
        .collect();
        let map2: HashMap<NameOrId, Object> = vec![
            (NameOrId::from("age".to_string()), 26.into()),
            (NameOrId::from("birthday".to_string()), 19950816.into()),
            (
                NameOrId::from("name".to_string()),
                "Nancy".to_string().into(),
            ),
        ]
        .into_iter()
        .collect();
        Vertices {
            vec: vec![
                Vertex::new(DynDetails::new(DefaultDetails::with_property(
                    1,
                    Label::from(9),
                    map1,
                ))),
                Vertex::new(DynDetails::new(DefaultDetails::with_property(
                    2,
                    Label::from(11),
                    map2,
                ))),
            ],
        }
    }

    #[test]
    fn test_eval_simple() {
        let cases: Vec<&str> = vec![
            "7 + 3",          // 10
            "7.0 + 3",        // 10.0
            "7 * 3",          // 21
            "7 / 3",          // 2
            "7 ^ 3",          // 343
            "7 ^ -3",         // 1 / 343
            "7 % 3",          // 1
            "7 -3",           // 4
            "-3 + 7",         // 4
            "-3",             // -3
            "-3.0",           // -3.0
            "false",          // false
            "!true",          // false
            "!10",            // false
            "!0",             // true
            "true || true",   // true
            "true || false",  // true
            "false || false", // false
            "true && true",   // true
            "true && false",  // false
            "1 > 2",          // false
            "1 < 2",          // true
            "1 >= 2",         // false
            "2 <= 2",         // true,
            "2 == 2",         // true
            "1.0 > 2.0",      // false
        ];

        let expected: Vec<Object> = vec![
            Object::from(10),
            Object::from(10.0),
            Object::from(21),
            Object::from(2),
            Object::from(343),
            Object::from(1.0 / 343.0),
            Object::from(1),
            Object::from(4),
            Object::from(4),
            Object::from(-3),
            Object::from(-3.0),
            Object::from(false),
            Object::from(false),
            Object::from(false),
            Object::from(true),
            Object::from(true),
            Object::from(true),
            Object::from(false),
            Object::from(true),
            Object::from(false),
            Object::from(false),
            Object::from(true),
            Object::from(false),
            Object::from(true),
            Object::from(true),
            Object::from(false),
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval =
                Evaluator::from_pb(to_suffix_expr_pb(tokenize(case).unwrap()).unwrap()).unwrap();
            assert_eq!(eval.eval::<(), NoneContext>(None).unwrap(), expected);
        }
    }

    #[test]
    fn test_eval_complex() {
        let cases: Vec<&str> = vec![
            "(-10)",                                 // -10
            "2 * 2 - 3",                             // 1
            "2 * (2 - 3)",                           // -2
            "6 / 2 - 3",                             // 0
            "6 / (2 - 3)",                           // -6
            "2 * 1e-3",                              // 0.002
            "1 > 2 && 1 < 3",                        // false
            "1 > 2 || 1 < 3",                        // true
            "2 ^ 10 > 10",                           // true
            "2 / 5 ^ 2",                             // 0
            "2.0 / 5 ^ 2",                           // 2.0 / 25
            "((1 + 2) * 3) / (7 * 8) + 12.5 / 10.1", // 1.2376237623762376
            "((1 + 2) * 3) / 7 * 8 + 12.5 / 10.1",   // 9.237623762376238
            "((1 + 2) * 3) / 7 * 8 + 12.5 / 10.1 \
                == ((1 + 2) * 3) / (7 * 8) + 12.5 / 10.1", // false
        ];

        let expected: Vec<Object> = vec![
            Object::from(-10),
            Object::from(1),
            Object::from(-2),
            Object::from(0),
            Object::from(-6),
            Object::from(0.002),
            Object::from(false),
            Object::from(true),
            Object::from(true),
            Object::from(0),
            Object::from(2.0 / 25.0),
            Object::from(1.2376237623762376),
            Object::from(9.237623762376238),
            Object::from(false),
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval =
                Evaluator::from_pb(to_suffix_expr_pb(tokenize(case).unwrap()).unwrap()).unwrap();
            assert_eq!(eval.eval::<(), NoneContext>(None).unwrap(), expected);
        }
    }

    #[test]
    fn test_eval_variable() {
        // [v0: id = 1, label = 9, age = 31, birthday = 19900416]
        // [v1: id = 2, label = 11, age = 26, birthday = 19950816]
        let ctxt = prepare_context();
        let cases: Vec<&str> = vec![
            "@0.ID",                                       // 1
            "@1.LABEL",                                    // 1
            "@0.ID < @1.ID",                               // true
            "@0.birthday > @1.birthday",                   // false
            "@0.LABEL == @1.LABEL",                        // false
            "@0.name != @1.name",                          // true
            "@0.name == \"John\"",                         // true
            "@0.name == \"John\" && @1.name == \"Jimmy\"", // false
            "@0.age + @0.birthday / 10000 == \
                @1.age + @1.birthday / 10000", // true
        ];

        let expected: Vec<Object> = vec![
            Object::from(1),
            Object::from(11),
            Object::from(true),
            Object::from(false),
            Object::from(false),
            Object::from(true),
            Object::from(true),
            Object::from(false),
            Object::from(true),
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval =
                Evaluator::from_pb(to_suffix_expr_pb(tokenize(case).unwrap()).unwrap()).unwrap();
            assert_eq!(eval.eval::<_, Vertices>(Some(&ctxt)).unwrap(), expected);
        }
    }

    #[test]
    fn test_eval_errors() {
        let cases: Vec<&str> = vec![
            "@2",
            "@2",
            "@1.nonexistent",
            "+",
            "1 + ",
            "1 1",
            "1 1 2",
            "1 1 + 2",
            "1 + @1.age * 1 1 - 1 - 5",
        ];
        let ctxt = prepare_context();

        let expected: Vec<ExprError> = vec![
            // Evaluate variable without providing the context
            ExprError::MissingContext(
                InnerOpr::Var {
                    tag: 2.into(),
                    prop_key: None,
                }
                .into(),
            ),
            // obtain non-value from the context
            ExprError::NoneOperand(
                InnerOpr::Var {
                    tag: 2.into(),
                    prop_key: None,
                }
                .into(),
            ),
            // obtain non-value from the context
            ExprError::NoneOperand(
                InnerOpr::Var {
                    tag: 1.into(),
                    prop_key: Some(PropKey::Key("nonexistent".to_string().into())),
                }
                .into(),
            ),
            // try to evaluate neither a variable nor a const
            ExprError::UnmatchedOperator(InnerOpr::Arith(pb::Arithmetic::Add).into()),
            ExprError::MissingOperands(InnerOpr::Arith(pb::Arithmetic::Add).into()),
            ExprError::OtherErr("invalid expression".to_string()),
            ExprError::OtherErr("invalid expression".to_string()),
            ExprError::OtherErr("invalid expression".to_string()),
            ExprError::OtherErr("invalid expression".to_string()),
        ];

        let mut is_context = false;
        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval =
                Evaluator::from_pb(to_suffix_expr_pb(tokenize(case).unwrap()).unwrap()).unwrap();
            assert_eq!(
                if is_context {
                    eval.eval::<_, Vertices>(Some(&ctxt)).err().unwrap()
                } else {
                    eval.eval::<_, NoneContext>(None).err().unwrap()
                },
                expected
            );
            is_context = true;
        }
    }
}
