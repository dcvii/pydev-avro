use avro_rs::{Codec, Reader, Schema, Writer};
use reqwest::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

#[derive(Debug, Deserialize, Serialize)]
struct Record {
    // Define the fields of your JSON records based on the API response
    field1: String,
    field2: i32,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let api_url = "https://api.example.com/your/endpoint"; // Replace with your API endpoint
    let json_records = get_api_records(api_url).await?;

    // Export to a flat file of JSON
    export_json_records_to_file(&json_records, "json_records.json")?;

    // Export to an Avro schema file
    let avro_schema = generate_avro_schema();
    export_avro_schema_to_file(&avro_schema, "avro_schema.avsc")?;

    // Export to multiple Avro records
    export_json_records_to_avro(&json_records, &avro_schema, "avro_records.avro")?;

    Ok(())
}

async fn get_api_records(url: &str) -> Result<Vec<Record>, Error> {
    let records = reqwest::get(url).await?.json::<Vec<Record>>().await?;
    Ok(records)
}

fn export_json_records_to_file(json_records: &[Record], filename: &str) -> std::io::Result<()> {
    let json_str = serde_json::to_string_pretty(json_records)?;
    let mut file = File::create(filename)?;
    file.write_all(json_str.as_bytes())?;
    Ok(())
}

fn generate_avro_schema() -> Schema {
    let raw_schema = r#"
    {
        "type": "record",
        "name": "Record",
        "fields": [
            {"name": "field1", "type": "string"},
            {"name": "field2", "type": "int"}
        ]
    }
    "#;
    Schema::parse_str(raw_schema).unwrap()
}

fn export_avro_schema_to_file(schema: &Schema, filename: &str) -> std::io::Result<()> {
    let mut file = File::create(filename)?;
    file.write_all(schema.to_string().as_bytes())?;
    Ok(())
}

fn export_json_records_to_avro(
    json_records: &[Record],
    schema: &Schema,
    filename: &str,
) -> std::io::Result<()> {
    let mut file = File::create(filename)?;

    let mut writer = Writer::with_codec(schema, &mut file, Codec::Deflate);

    for record in json_records {
        let value = Value::Record(vec![
            ("field1".to_string(), Value::String(record.field1.clone())),
            ("field2".to_string(), Value::Int(record.field2)),
        ]);
        writer.append(value
