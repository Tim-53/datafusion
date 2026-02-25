use std::any::Any;
use std::fmt::Display;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Schema};
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ColumnExpr {
    index: usize,
    name: String,
}

impl ColumnExpr {
    pub fn new(name: impl Into<String>, index: usize) -> Self {
        ColumnExpr {
            index,
            name: name.into(),
        }
    }
}

impl PhysicalExpr for ColumnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        Ok(input_schema.field(self.index).data_type().clone())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Array(Arc::clone(batch.column(self.index))))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if !children.is_empty() {
            return datafusion_common::plan_err!("ColumnExpr cannot have children");
        }
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Display for ColumnExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}
