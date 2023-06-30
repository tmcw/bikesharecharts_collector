use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::Timestamp;
use arrow_array::builder::PrimitiveBuilder;
use arrow_array::types::{TimestampMillisecondType, TimestampSecondType, UInt16Type};
use arrow_array::{
    ArrayRef, Date64Array, RecordBatch, Time64MicrosecondArray, TimestampMillisecondArray,
    TimestampSecondArray,
};
use flate2::bufread;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
struct Station {
    #[serde(skip)]
    legacy_id: String,
    last_reported: u64,
    num_ebikes_available: u16,
    num_bikes_available: u16,
    is_returning: u32,
    #[serde(skip)]
    eightd_has_available_keys: bool,
    num_docks_available: u16,
    num_docks_disabled: u16,
    is_installed: u32,
    num_bikes_disabled: u16,
    station_id: String,
    station_status: String,
    is_renting: u32,
}

#[derive(Debug, Deserialize)]
struct Data {
    stations: Vec<Station>,
}

#[derive(Debug, Deserialize)]
struct StationStatus {
    data: Data,
    last_updated: i64,
    #[serde(skip)]
    ttl: u32,
}

fn main() {
    let paths = fs::read_dir("./station_status").unwrap();

    let file = File::create("data.parquet").unwrap();

    let station_ids = Field::new("station_ids", DataType::UInt16, false);
    let num_bikes_available = Field::new("num_bikes_available", DataType::UInt16, false);
    let num_ebikes_available = Field::new("num_ebikes_available", DataType::UInt16, false);
    let num_docks_available = Field::new("num_docks_available", DataType::UInt16, false);
    let times = Field::new(
        "times",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    );

    let mut id_legend: HashMap<String, u16> = HashMap::new();
    let mut id_counter: u16 = 0;

    let schema = Schema::new(vec![
        station_ids,
        num_bikes_available,
        num_ebikes_available,
        num_docks_available,
        times,
    ]);

    let props = WriterProperties::builder().build();

    let mut writer = ArrowWriter::try_new(file, schema.into(), props.into()).unwrap();

    for entry in paths {
        let input = BufReader::new(File::open(entry.unwrap().path()).unwrap());
        let mut decoder = bufread::GzDecoder::new(input);
        let status: StationStatus = serde_json::from_reader(&mut decoder).unwrap();
        let stations: Vec<Station> = status
            .data
            .stations
            .into_iter()
            .filter(|station| station.station_status == "active")
            .collect();

        println!("Station count: {}", status.last_updated);

        // Warning: You can specify Second here, and it won't work!
        // https://github.com/apache/arrow-rs/issues/1920#issuecomment-1164220176
        let mut times = PrimitiveBuilder::<TimestampMillisecondType>::new();
        let mut station_ids = PrimitiveBuilder::<UInt16Type>::new();
        let mut num_bikes_available = PrimitiveBuilder::<UInt16Type>::new();
        let mut num_ebikes_available = PrimitiveBuilder::<UInt16Type>::new();
        let mut num_docks_available = PrimitiveBuilder::<UInt16Type>::new();

        for station in &stations {
            times.append_value(status.last_updated * 1000);
            let station_id = id_legend
                .entry(station.station_id.clone().into())
                .or_insert_with(|| {
                    id_counter = id_counter + 1;
                    id_counter
                });
            station_ids.append_value(*station_id);
            num_bikes_available.append_value(station.num_bikes_available);
            num_ebikes_available.append_value(station.num_ebikes_available);
            num_docks_available.append_value(station.num_docks_available);
        }

        let batch = RecordBatch::try_from_iter(vec![
            ("station_ids", Arc::new(station_ids.finish()) as ArrayRef),
            (
                "num_bikes_available",
                Arc::new(num_bikes_available.finish()) as ArrayRef,
            ),
            (
                "num_ebikes_available",
                Arc::new(num_ebikes_available.finish()) as ArrayRef,
            ),
            (
                "num_docks_available",
                Arc::new(num_docks_available.finish()) as ArrayRef,
            ),
            ("time", Arc::new(times.finish()) as ArrayRef),
        ])
        .unwrap();

        writer.write(&batch).expect("Writing batch");
    }

    println!("Done!");
    // writer must be closed to write footer
    writer.close().unwrap();
}
