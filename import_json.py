import json
import requests
from fastavro import writer, parse_schema
import avro.schema


def get_api_records(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def export_json_records_to_file(json_records, filename):
    with open(filename, 'w') as f:
        json.dump(json_records, f, indent=2)


def generate_avro_schema():
    raw_schema = {
        "type": "record",
        "name": "Record",
        "fields": [
            {"name": "field1", "type": "string"},
            {"name": "field2", "type": "int"},
        ],
    }
    return avro.schema.Parse(json.dumps(raw_schema))


def export_avro_schema_to_file(schema, filename):
    with open(filename, 'w') as f:
        f.write(str(schema))


def export_json_records_to_avro(json_records, schema, filename):
    parsed_schema = parse_schema(schema.to_json())

    records = []
    for record in json_records:
        avro_record = {k: record[k] for k in schema.names}
        records.append(avro_record)

    with open(filename, 'wb') as f:
        writer(f, parsed_schema, records)


def main():
    api_url = "https://api.example.com/your/endpoint"  # Replace with your API endpoint
    json_records = get_api_records(api_url)

    # Export to a flat file of JSON
    export_json_records_to_file(json_records, "json_records.json")

    # Export to an Avro schema file
    avro_schema = generate_avro_schema()
    export_avro_schema_to_file(avro_schema, "avro_schema.avsc")

    # Export to multiple Avro records
    export_json_records_to_avro(json_records, avro_schema, "avro_records.avro")


if __name__ == "__main__":
    main()
