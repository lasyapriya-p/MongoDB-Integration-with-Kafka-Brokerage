import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient


def delivery_report(
    err, msg
):  # It reports the delivery status of the data to kafka cluster
    if err is not None:  # If some error is found
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print(
        "User record successfully produced to {} [{}] at offset {}".format(
            msg.topic(), msg.partition(), msg.offset()
        )  # If message was successfully sent to cluster
    )


kafka_config = {
    "bootstrap.servers": "pkc-4r087.us-west2.gcp.confluent.cloud:9092",  # The host url of kafka server(cloud)
    "sasl.mechanisms": "PLAIN",  # SASL mechanisms for authentication
    "security.protocol": "SASL_SSL",  # Security Protocol
    "sasl.username": "AW4K5BYQHUA4CBC3",  # SASL USERNAME(API)
    "sasl.password": "bfW1DQgEjtVN7OO6eE/ATJ4gI3gCNF95pBqQ0SMUoV1WKuAdSoxH9anAP2EmQC5t",  # SASL PASSWORD(API)
}
# Create a Schema Registry client which manages the updated schema
schema_registry_client = SchemaRegistryClient(
    {
        "url": 'https://psrc-5j7x8.us-central1.gcp.confluent.cloud',  # The host url of schema registry
        "basic.auth.user.info": "{}:{}".format(
            "QJP6KGEGC2IJWZON",  # Schema Registry API key
            'nH1Y+rkfgWTpzJRAwiV90nwo/J9b/WzdACyo8Oy6psLn6e6G1vczMSiWnSIP3jix',  # Schema registry API value/secret
        ),
    }
)

# Fetch the latest Avro schema for the value
subject_name = "lasya-value"  # topic_name+(-value)
schema_str = schema_registry_client.get_latest_version(
    subject_name
).schema.schema_str  # It gives the latest updated schema version as string(actual one in ui)

# Create Avro Serializer for the value
key_serializer = StringSerializer("utf_8")  # Serializer for keys, encoded as strings
avro_serializer = AvroSerializer(
    schema_registry_client, schema_str
)  # Serializer for Avro schema

# Define the SerializingProducer object which publishes data
producer = SerializingProducer(
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
        "key.serializer": key_serializer,  # Key will be serialized as a string
        "value.serializer": avro_serializer,  # Value will be serialized as Avro
    }
)
df = pd.read_csv("sales_data.csv")#Read csv file as df
df.fillna("null")#Fill null values

for index, row in df.iterrows():
    sales_data = {
        "ORDERNUMBER":str(row["ORDERNUMBER"]),
        "QUANTITYORDERED":str(row["QUANTITYORDERED"]),
        "PRICEEACH":str(row["PRICEEACH"]),
        "ORDERLINENUMBER":str(row["ORDERLINENUMBER"]),
        "SALES":str(row["SALES"]),
        "ORDERDATE":str(row["ORDERDATE"]),
        "STATUS":str(row["STATUS"]),
        "QTR_ID":str(row["QTR_ID"]),
        "MONTH_ID":str(row["MONTH_ID"]),
        "YEAR_ID":str(row["YEAR_ID"])
    }

    producer.produce(
        topic="lasya",
        key=str(row["ORDERNUMBER"]),
        value=sales_data,
        on_delivery=delivery_report,
    )# Push the record into the specified topic with the given key-value pair
    producer.flush()# Flush the producer buffer to ensure all messages are sent
    print(index)#Printing the doc index
print("All Data successfully published to Kafka")
