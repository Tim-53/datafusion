use std::{any::Any, cmp::min, sync::Arc, task::Poll};

use arrow::array::{ArrayBuilder, ArrayRef, Float64Builder, Int32Builder, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_catalog::TableProvider;
use datafusion_execution::RecordBatchStream;
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_session::Session;
use futures::Stream;
use rand::{Rng, SeedableRng, rngs::StdRng};

use crate::error::Result;

#[derive(Debug)]
struct RandomTable {
    num_rows: usize,
    schema: SchemaRef,
    seed: usize,
}

#[async_trait]
impl TableProvider for RandomTable {
    #[doc = " Returns the table provider as [`Any`] so that it can be"]
    #[doc = " downcast to a specific implementation."]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[doc = " Get a reference to the schema for this table"]
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    #[doc = " Get the type of this table for metadata/catalog purposes."]
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RandomTableExec::new(
            Arc::clone(&self.schema),
            self.seed,
            limit.unwrap_or(self.num_rows),
            projection.cloned(),
        )))
    }
}

#[derive(Debug, Clone)]
struct RandomTableExec {
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    num_rows: usize,
    plan_properties: PlanProperties,
    seed: usize,
}

impl RandomTableExec {
    pub fn new(
        schema: SchemaRef,
        seed: usize,
        num_rows: usize,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let fields: Vec<&arrow_schema::Field> = match &projection {
            Some(indices) => indices.iter().map(|i| schema.field(*i)).collect(),
            None => schema.fields().iter().map(|f| f.as_ref()).collect(),
        };

        let projected_fields: Vec<Arc<arrow_schema::Field>> =
            fields.iter().map(|f| Arc::new((*f).clone())).collect();
        let projected_schema = Arc::new(Schema::new(projected_fields));

        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        RandomTableExec {
            schema: projected_schema.clone(),
            num_rows,
            plan_properties: properties,
            seed,
            projection,
        }
    }
}

impl DisplayAs for RandomTableExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RandomTableExec")
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for RandomTableExec {
    fn name(&self) -> &str {
        "random table"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<datafusion_execution::SendableRecordBatchStream> {
        Ok(Box::pin(RandomStream::new(
            Arc::clone(&self.schema),
            self.seed,
            self.num_rows,
        )))
    }
}

struct RandomStream {
    schema: SchemaRef,
    produced: usize,
    limit: usize,
    batch_size: usize,
    rng_gen: StdRng,
}

impl RandomStream {
    pub fn new(schema: SchemaRef, seed: usize, num_rows: usize) -> Self {
        RandomStream {
            schema,
            produced: 0,
            limit: num_rows,
            batch_size: 128,
            rng_gen: StdRng::seed_from_u64(seed.try_into().unwrap()),
        }
    }
}

impl RecordBatchStream for RandomStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for RandomStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.produced >= self.limit {
            return Poll::Ready(None);
        }

        let mut builders: Vec<Box<dyn ArrayBuilder>> = vec![];
        let batch_size = min(self.batch_size, self.limit - self.produced);

        for field in self.schema().fields.iter() {
            let builder = match field.data_type() {
                DataType::Int32 => {
                    let mut b = Int32Builder::new();
                    for _ in 0..batch_size {
                        b.append_value(self.rng_gen.random::<i32>());
                    }
                    Box::new(b) as Box<dyn ArrayBuilder>
                }
                DataType::Float64 => {
                    let mut b = Float64Builder::new();
                    for _ in 0..batch_size {
                        b.append_value(self.rng_gen.random::<f64>());
                    }
                    Box::new(b) as Box<dyn ArrayBuilder>
                }
                _ => todo!(),
            };
            builders.push(builder);
        }

        let columns: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();

        let result = RecordBatch::try_new(self.schema.clone(), columns)?;
        self.produced += batch_size;
        Poll::Ready(Some(Ok(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::SessionContext;
    use arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn test_register_random_table() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let table = RandomTable {
            schema: Arc::clone(&schema),
            num_rows: 100,
            seed: 42,
        };
        let ctx = SessionContext::new();
        ctx.register_table("random", Arc::new(table)).unwrap();

        let registered = ctx.table("random").await.unwrap();
        assert_eq!(registered.schema().field(0).name(), "a");
        let df = ctx.sql("SELECT a FROM random Where a > 10").await.unwrap();
        let _results = df.collect().await.unwrap();
        println!("hi");
        // println!("{:?}", results)
    }
}
