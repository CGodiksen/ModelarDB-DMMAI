use std::iter;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{RecordBatch, StringArray};
use arrow::compute;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use futures_util::TryStreamExt;
use modelardb_types::types::{ArrowTimestamp, ArrowValue, TimestampBuilder};

/// Read the wind data from a Parquet file and return it as a single RecordBatch.
pub async fn read_wind_data() -> RecordBatch {
    let file = tokio::fs::File::open("data/wind_cleaned.parquet")
        .await
        .unwrap();

    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();

    let stream = builder.build().unwrap();
    let record_batches = stream.try_collect::<Vec<_>>().await.unwrap();

    compute::concat_batches(&record_batches[0].schema(), &record_batches).unwrap()
}

/// Return the schema of the wind data table.
pub fn table_schema() -> Schema {
    Schema::new(vec![
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
    ])
}

/// Given a record batch with the fields in the wind table, generate time series data by adding a
/// timestamp column with evenly spaced timestamps starting from the current time and adding
/// two tag columns `park_id` and `windmill_id` with constant values.
pub async fn generate_time_series_data(
    data_points: RecordBatch,
    park_id: &str,
    windmill_id: &str,
) -> RecordBatch {
    let mut timestamps = TimestampBuilder::with_capacity(data_points.num_rows());

    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap();

    let mut next_timestamp: i64 = since_the_epoch.as_micros() as i64;
    let step = (Duration::from_secs(2).as_micros() as i64) / (data_points.num_rows() as i64);

    for _ in 0..data_points.num_rows() {
        timestamps.append_value(next_timestamp);
        next_timestamp += step;
    }

    let park_id_array: StringArray = iter::repeat(Some(park_id))
        .take(data_points.num_rows())
        .collect();

    let windmill_id_array: StringArray = iter::repeat(Some(windmill_id))
        .take(data_points.num_rows())
        .collect();

    RecordBatch::try_new(
        Arc::new(table_schema()),
        vec![
            Arc::new(timestamps.finish()),
            Arc::new(park_id_array),
            Arc::new(windmill_id_array),
            data_points.column(0).clone(),
            data_points.column(1).clone(),
            data_points.column(2).clone(),
            data_points.column(3).clone(),
            data_points.column(4).clone(),
            data_points.column(5).clone(),
            data_points.column(6).clone(),
            data_points.column(7).clone(),
            data_points.column(8).clone(),
            data_points.column(9).clone(),
        ],
    )
    .unwrap()
}
