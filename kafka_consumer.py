from pymongo import MongoClient
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

kafka_config = {
    "bootstrap.servers": "pkc-4r087.us-west2.gcp.confluent.cloud:9092",  # The host url of kafka server(cloud)
    "sasl.mechanisms": "PLAIN",  # SASL mechanisms for authentication
    "security.protocol": "SASL_SSL",  # Security Protocol
    "sasl.username": "AW4K5BYQHUA4CBC3",  # SASL USERNAME(API)
    "sasl.password": "bfW1DQgEjtVN7OO6eE/ATJ4gI3gCNF95pBqQ0SMUoV1WKuAdSoxH9anAP2EmQC5t",  # SASL PASSWORD(API)
    "group.id": "group11",  # Groupid of the consumer
    "auto.offset.reset": "earliest",  # Consuming latest data we can also use 'earliest'
}
# Create a Schema Registry client which manages the updated schema
schema_registry_client = SchemaRegistryClient(
    {
        "url": "https://psrc-5j7x8.us-central1.gcp.confluent.cloud",  # The host url of schema registry
        "basic.auth.user.info": "{}:{}".format(
            "QJP6KGEGC2IJWZON",  # Schema Registry API key
            "nH1Y+rkfgWTpzJRAwiV90nwo/J9b/WzdACyo8Oy6psLn6e6G1vczMSiWnSIP3jix",  # Schema registry API value/secret
        ),
    }
)
# Fetch the latest Avro schema for the value
subject_name = "lasya-value"  # topic_name+(-value)
schema_str = schema_registry_client.get_latest_version(
    subject_name
).schema.schema_str  # It gives the latest updated schema version as string(actual one in ui)
# Create Avro Deserializer for the value
key_deserializer = StringDeserializer(
    "utf_8"
)  # DeSerializer for keys, encoded as strings
avro_deserializer = AvroDeserializer(
    schema_registry_client, schema_str
)  # DeSerializer for Avro schema
# Define the DeserializingConsumer object
consumer = DeserializingConsumer(
    {
        "bootstrap.servers": kafka_config["bootstrap.servers"],  # Kafka host server url
        "security.protocol": kafka_config[
            "security.protocol"
        ],  # Security protocol configuration
        "sasl.mechanisms": kafka_config[
            "sasl.mechanisms"
        ],  # SASL mechanisms for authentication
        "sasl.username": kafka_config["sasl.username"],  # SASL USERNAME(API)
        "sasl.password": kafka_config["sasl.password"],  # SASL PASSWORD(API)
        "key.deserializer": key_deserializer,  # Key will be Deserialized as a string
        "value.deserializer": avro_deserializer,  # Value will be deserialized as Avro
        "group.id": kafka_config["group.id"],  # Groupid of the consumer
        "auto.offset.reset": kafka_config["auto.offset.reset"],  # Consuming data offset
        # 'enable.auto.commit': True, #Enabling auto commit
        # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds which is manual and sync()
    }
)
# MongoDB Setup
client = MongoClient(
    "mongodb+srv://krishnasairaj:3TLvpofXOtrP94NE@cluster1.katgqk9.mongodb.net/?retryWrites=true&w=majority"
)#URL Of the MongoDB Server
db = client["sales_data"]#Database name
collection = db["lasya"]#Collection name
consumer.subscribe(["lasya"])  # Consumer object is subscribing to kafka topic
try:
    while True:
        message = consumer.poll(1.0)  # Waiting time for message (in seconds)
        
        if message is None:  # If message is not recieved
            continue
        if message.error():  # If error occured
            print("Consumer error: {}".format(message.error()))
            continue
        
        if 'BookingID' not in value or value["BookingID"] is None:#Skip if BookingID is Null Value
            print("Skipped Due to missing bookingid")
            continue
        if not isinstance(value["BookingID"], str):#To check is BookingID a String value
            print("Skipped Due to booking id is not a string")
            continue
        if value["GpsProvider"] is None or value["vehicle_no"] is None:#Skip if GpsProvider is Null Value
            print("Skipped Due to GpsProvider or Vechile_no missing")
            continue
        booking_id_check = collection.find_one({"BookingID": value["BookingID"]})
        if booking_id_check:#If the record is already found in collection
            print(f"The bookingid:{value['BookingID']} is already inserted")
        else:
            collection.insert_one(value)#Insert one record into collection
            print("Successfully consumed record with key {}".format(message.key()))
except KeyboardInterrupt:  # To interupt the consumer process
    pass
finally:  # Finally close the consumer connection
    consumer.close()
