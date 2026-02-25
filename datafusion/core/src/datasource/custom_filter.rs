use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, pin::Pin};

use arrow::array::{ArrayRef, BooleanArray, MutableArrayData, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::{Result, plan_err};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::Stream;

#[derive(Debug)]
struct CustomFilter {
    schema: SchemaRef,
    expression: Arc<dyn PhysicalExpr>,
    child: Arc<dyn ExecutionPlan>,
}

impl CustomFilter {
    pub fn try_new(
        expression: Arc<dyn PhysicalExpr>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<CustomFilter> {
        if children.len() != 1 {
            return plan_err!(
                "CustomFilter requires exactly 1 child, got {}",
                children.len()
            );
        }

        let child = Arc::clone(&children[0]);

        let schema = child.schema();

        let data_type = expression.data_type(&schema)?;
        if data_type != DataType::Boolean {
            return plan_err!(
                "CustomFilter expression must return Boolean, got {:?}",
                data_type
            );
        }

        Ok(CustomFilter {
            child,
            expression,
            schema,
        })
    }
}

impl ExecutionPlan for CustomFilter {
    fn name(&self) -> &str {
        "just a custom filter"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.child.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        Ok(Box::pin(CustomFilterStream::new(
            child_stream,
            Arc::clone(&self.schema),
            Arc::clone(&self.expression),
        )))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_filter = CustomFilter::try_new(Arc::clone(&self.expression), children)?;
        Ok(Arc::new(new_filter))
    }
}

impl DisplayAs for CustomFilter {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CustomFilter: {}", self.expression)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "predicate={}", self.expression)
            }
        }
    }
}

struct CustomFilterStream {
    child_stream: SendableRecordBatchStream,
    schema: SchemaRef,
    expression: Arc<dyn PhysicalExpr>,
}

impl CustomFilterStream {
    pub fn new(
        child_stream: SendableRecordBatchStream,
        schema: SchemaRef,
        expression: Arc<dyn PhysicalExpr>,
    ) -> Self {
        CustomFilterStream {
            child_stream,
            schema,
            expression,
        }
    }
}

impl RecordBatchStream for CustomFilterStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for CustomFilterStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.child_stream.as_mut().poll_next(cx)? {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(batch)) => {
                // apply filter operation to batch
                let filter_mask = self.expression.evaluate(&batch)?;
                let array = filter_mask.into_array(batch.num_rows())?;
                let bool_array = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Internal(
                            "filter mask is not a BooleanArray".to_string(),
                        )
                    })?;

                println!("{:?}", bool_array);

                let mut filtered_columns: Vec<ArrayRef> = vec![];
                for col in batch.columns().iter() {
                    let col_data = col.to_data();
                    let mut filtered_col =
                        MutableArrayData::new(vec![&col_data], false, batch.num_rows());

                    for (i, value) in bool_array.iter().enumerate() {
                        if value == Some(true) {
                            filtered_col.extend(0, i, i + 1);
                        }
                    }

                    filtered_columns
                        .push(arrow::array::make_array(filtered_col.freeze()));
                }

                let result_batch =
                    RecordBatch::try_new(self.schema.clone(), filtered_columns)?;
                Poll::Ready(Some(Ok(result_batch)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::datasource::physical_exprs::binary_expr::CustomBinaryExpr;
    use crate::datasource::physical_exprs::column_expr::ColumnExpr;
    use crate::datasource::physical_exprs::scalar_expr::ScalarExpr;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_execution::TaskContext;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::BinaryExpr;
    use datafusion_physical_plan::collect;
    use datafusion_physical_plan::test::TestMemoryExec;

    #[tokio::test]
    async fn test_custom_filter() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int32Array::from(vec![5, 4, 3, 2, 1])),
            ],
        )?;

        let input =
            TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;

        // predicate: a > 2
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(CustomBinaryExpr::new(
            Arc::new(ColumnExpr::new("a", 0)),
            Operator::Gt,
            Arc::new(ScalarExpr::new(ScalarValue::Int32(Some(2)))),
        ));

        let filter = CustomFilter::try_new(predicate, vec![input])?;
        let results = collect(Arc::new(filter), Arc::new(TaskContext::default())).await?;

        for batch in &results {
            println!("{:?}", batch);
        }

        Ok(())
    }
}
