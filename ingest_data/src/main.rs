use std::collections::HashMap;

use arrow::compute;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use futures_util::TryStreamExt;
use modelardb_embedded::TableType;
use modelardb_embedded::operations::Operations;
use modelardb_embedded::operations::client::{Client, Node};
use modelardb_types::types::{ArrowTimestamp, ArrowValue};

async fn create_table(mut modelardb_client: Client) {
    let schema = Schema::new(vec![
        Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
        Field::new("park_id", DataType::Utf8, false),
        Field::new("windmill_id", DataType::Utf8, false),
        Field::new("wind_speed", ArrowValue::DATA_TYPE, false),
        Field::new("pitch_angle", ArrowValue::DATA_TYPE, false),
        Field::new("rotor_speed", ArrowValue::DATA_TYPE, false),
        Field::new("active_power", ArrowValue::DATA_TYPE, false),
        Field::new("cos_nacelle_dir", ArrowValue::DATA_TYPE, false),
        Field::new("sin_nacelle_dir", ArrowValue::DATA_TYPE, false),
        Field::new("cos_wind_dir", ArrowValue::DATA_TYPE, false),
        Field::new("sin_wind_dir", ArrowValue::DATA_TYPE, false),
        Field::new("cor_nacelle_direction", ArrowValue::DATA_TYPE, false),
        Field::new("cor_wind_direction", ArrowValue::DATA_TYPE, false),
    ]);

    let table_type = TableType::TimeSeriesTable(schema, HashMap::new(), HashMap::new());

    modelardb_client
        .create("wind", table_type)
        .await
        .unwrap();
}

fn main() {
    println!("Hello, world!");
async fn read_wind_data() -> RecordBatch {
    let file = tokio::fs::File::open("data/wind_cleaned.parquet").await.unwrap();
    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();

    let stream = builder.build().unwrap();
    let record_batches = stream.try_collect::<Vec<_>>().await.unwrap();

    compute::concat_batches(&record_batches[0].schema(), &record_batches).unwrap()
}
}
