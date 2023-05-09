import json
import requests
import io
import fastavro
from fastavro import parse_schema
from confluent_kafka import avro
from confluent_kafka.avro import CachedSchemaRegistryClient
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import AvroSerializer
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient





# Define the Avro schema for the data
schema_str = '''
{
 "type": "record",
 "name": "AircraftState",
 "fields": [
 {"name": "icao24", "type": "string"},
 {"name": "callsign", "type": ["null", "string"]},
 {"name": "origin_country", "type": "string"},
 {"name": "time_position", "type": ["null", "long"]},
 {"name": "last_contact", "type": "long"},
 {"name": "longitude", "type": ["null", "double"]},
 {"name": "latitude", "type": ["null", "double"]},
 {"name": "baro_altitude", "type": ["null", "double"]},
 {"name": "on_ground", "type": "boolean"},
 {"name": "velocity", "type": ["null", "double"]},
 {"name": "true_track", "type": ["null", "double"]},
 {"name": "vertical_rate", "type": ["null", "double"]},
 {"name": "sensors", "type": ["null", {"type": "array", "items": "int"}]},
 {"name": "geo_altitude", "type": ["null", "double"]},
 {"name": "squawk", "type": ["null", "string"]},
 {"name": "spi", "type": "boolean"},
 {"name": "position_source", "type": "int"}
 ]
}
'''
schema = avro.loads(schema_str)

# schema = parse_schema(json.loads(schema_str))

# producer = AvroProducer({
#     'bootstrap.servers': '52.203.71.36:9092',
#     'schema.registry.url': 'http://52.203.71.36:8081'
#     }, default_key_schema=None, default_value_schema=schema, schema_registry=avro.loads(schema_str))


# Configure the Kafka producer


producer = AvroProducer({
 'bootstrap.servers': '52.203.71.36:9092',
 'schema.registry.url': 'http://52.203.71.36:8081'
}, default_key_schema=None, default_value_schema=schema, schema_registry=schema_registry_client)


# Create the schema registry client
schema_registry_client = SchemaRegistryClient({'url': 'http://52.203.71.36:8081'})

# Create the serializer
# serializer = AvroSerializer(schema, schema_registry_client)
message_serializer = MessageSerializer(schema_registry_client)


# Send the JSON data to the Kafka topic
def send_to_kafka(topic, data):
    message = {'icao24': data[0], 'callsign': data[1], 'origin_country': data[2], 'time_position': data[3], 'last_contact': data[4], 'longitude': data[5], 'latitude': data[6], 'baro_altitude': data[7], 'on_ground': data[8], 'velocity': data[9], 'true_track': data[10], 'vertical_rate': data[11], 'sensors': data[12], 'geo_altitude': data[13], 'squawk': data[14], 'spi': data[15], 'position_source': data[16]}
    # value = serializer.encode_record_with_schema(topic, schema, message)
    # value = message_serializer.encode_record_with_schema(topic, schema, message)
    with io.BytesIO() as output:
        fastavro.schemaless_writer(output, schema, message)
        value = output.getvalue()
        producer.produce(topic=topic, value=value)
        producer.flush()
        print('Data sent to Kafka topic:', topic)



# Fetch the JSON data using curl and send it to Kafka



def fetch_and_send_data(url, topic):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()['states']
        for item in data:
            send_to_kafka(topic, item)
    else:
        print('Failed to fetch data with status code:', response.status_code)


if __name__ == '__main__':
 # URL to fetch the JSON data from
 url = 'https://opensky-network.org/api/states/all'

def main():
    # Kafka topic to send the data to
    topic = 'OpenSkyStatesAVRO'

    # Fetch the data and send it to Kafka
    fetch_and_send_data(url, topic)

    schema_registry_client = CachedSchemaRegistryClient({
    'url': 'http://52.203.71.36:8081'
    })
