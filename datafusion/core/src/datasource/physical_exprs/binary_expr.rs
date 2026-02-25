use std::{fmt::Display, sync::Arc};

use crate::arrow::compute::kernels::cmp::{gt, gt_eq, lt, lt_eq};
use crate::datasource::physical_exprs::binary_expr;
use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::{Result, plan_err};
use datafusion_expr::ColumnarValue;
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;

#[derive(Debug, Hash, Eq)]
pub struct CustomBinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    op: Operator,
    // data_type: DataType,
}

impl CustomBinaryExpr {
    pub fn new(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Self {
        CustomBinaryExpr { left, right, op }
    }

    pub fn try_new(
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        op: Operator,
    ) -> Result<Self> {
        Ok(CustomBinaryExpr::new(left, op, right))
    }

    fn compare(
        op: Operator,
        left: ColumnarValue,
        right: ColumnarValue,
    ) -> Result<BooleanArray> {
        let len = match (&left, &right) {
            (ColumnarValue::Array(l), ColumnarValue::Array(r)) => {
                if l.len() != r.len() {
                    return plan_err!(
                        "Arrays have different lengths: {} vs {}",
                        l.len(),
                        r.len()
                    );
                }
                l.len()
            }
            (ColumnarValue::Array(l), ColumnarValue::Scalar(_)) => l.len(),
            (ColumnarValue::Scalar(_), ColumnarValue::Array(r)) => r.len(),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_)) => 1,
        };
        let left_array = left.into_array(len)?;
        let right_array = right.into_array(len)?;

        let result = match op {
            Operator::Lt => lt(&left_array, &right_array)?,
            Operator::LtEq => lt_eq(&left_array, &right_array)?,
            Operator::Gt => gt(&left_array, &right_array)?,
            Operator::GtEq => gt_eq(&left_array, &right_array)?,
            _ => todo!(),
        };

        Ok(result)
    }
}

impl PhysicalExpr for CustomBinaryExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self, input_schema: &arrow::datatypes::Schema) -> Result<arrow::datatypes::DataType> {
        match self.op {
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            | Operator::Eq | Operator::NotEq => Ok(arrow::datatypes::DataType::Boolean),
            _ => self.left.data_type(input_schema),
        }
    }

    fn evaluate(&self, batch: &arrow::array::RecordBatch) -> Result<ColumnarValue> {
        let left_result = self.left.evaluate(batch)?;
        let right_result = self.right.evaluate(batch)?;

        let result = match self.op {
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                CustomBinaryExpr::compare(self.op, left_result, right_result)?
            }

            _ => todo!(),
        };

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        todo!()
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Display for CustomBinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

impl PartialEq for CustomBinaryExpr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.left, &other.left) && Arc::ptr_eq(&self.right, &other.right)
    }
}
