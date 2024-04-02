# MongoDB-Integration-with-Kafka-Brokerage

**Kafka-Driven Logistics Data Ingestion into MongoDB**

The project sets up a reliable pipeline that uses Kafka messaging to process sales data and stores it effectively in MongoDB. It integrates Avro schema for data serialisation and contains producer and consumer Kafka scripts.

### Features

1. **Kafka Integration:** Leverages Kafka messaging for handling sales data efficiently.
3. **Data Serialization:** Implements Avro schema for structured data handling, ensuring seamless serialization of data.
4. **File Inclusion:** Contains sales.csv for simplified data ingestion.
5. **Kafka Topic Creation:** Creates a Kafka topic named 'sales_data ' with 5 partitions, optimizing data distribution and processing.

### Technology Used

- **Python Scripts:** Includes kafka_producer.py for sending sales data to Kafka and kafka_consumer.py for processing data from Kafka and storing it in MongoDB.
- **Data Schema:** Avro schema represented ensuring structured data handling and seamless serialization.
- **File Contents:** Contains sales.csv, simplifying the ingestion of sales data.
- **Kafka Configuration:** 'sales_data' Kafka topic created with 5 partitions, optimizing data distribution and processing.
- **MongoDB:** Database utilized for storing the processed sales data efficiently.

This project facilitates the seamless processing and ingestion of sales data using Kafka messaging and MongoDB, enabling streamlined data management and analysis.

<div style="display:flex; justify-content: center;">
  <img width="380" alt="Screenshot 2024-01-05 141253" src="https://github.com/lasyapriya-p/MongoDB-Integration-with-Kafka-Brokerage/assets/113578942/ef212a75-d231-46f0-8c45-e948be2dc2aa">
<img width="380" alt="Screenshot 2024-01-05 140926" src="https://github.com/lasyapriya-p/MongoDB-Integration-with-Kafka-Brokerage/assets/113578942/66a10d08-06c7-47dc-a25f-829ed8061dbb">
</div>
