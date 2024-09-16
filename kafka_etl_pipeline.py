from kafka import KafkaConsumer
import json
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

# PostgreSQL connection details
DATABASE_URI = 'postgresql+psycopg2://user:password@localhost:5432/kafka_db'

# Kafka settings
KAFKA_TOPIC = 'public_topic'
KAFKA_SERVER = 'localhost:9092'

# Create PostgreSQL engine
engine = create_engine(DATABASE_URI)

def extract_data():
    """Extract data from Kafka topic."""
    consumer = KafkaConsumer(KAFKA_TOPIC,
                             bootstrap_servers=[KAFKA_SERVER],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    messages = []
    
    for message in consumer:
        # Append the message to the list
        messages.append(message.value)
        if len(messages) >= 100:  # Collect 100 messages at a time
            break
    
    # Save the raw Kafka messages to JSON
    with open('/tmp/kafka_messages.json', 'w') as f:
        json.dump(messages, f)

def transform_data():
    """Transform the raw Kafka data."""
    with open('/tmp/kafka_messages.json', 'r') as f:
        data = json.load(f)

    # Convert to DataFrame for transformation
    df = pd.DataFrame(data)

    # Example transformation: calculate mean of a numeric field (assuming 'value' field)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)

    # Save transformed data to CSV
    df.to_csv('/tmp/transformed_kafka_data.csv')

def load_data():
    """Load the transformed data into PostgreSQL."""
    df = pd.read_csv('/tmp/transformed_kafka_data.csv')

    # Load data into the 'kafka_data' table in PostgreSQL
    df.to_sql('kafka_data', engine, if_exists='append', index=False)

def visualize_data():
    """Visualize data using Matplotlib."""
    # Query the data from PostgreSQL
    df = pd.read_sql('SELECT * FROM kafka_data', engine)

    # Plot data (e.g., time series of a numeric field)
    df.plot(x='timestamp', y='value', kind='line')
    plt.title('Kafka Data Visualization')
    plt.xlabel('Timestamp')
    plt.ylabel('Value')
    plt.show()

# Define Airflow DAG
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

with DAG('kafka_etl_pipeline', start_date=datetime(2023, 9, 1), schedule_interval='@daily') as dag:

    extract_task = PythonOperator(
        task_id='extract_kafka_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_kafka_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_kafka_data',
        python_callable=load_data
    )
    
    visualize_task = PythonOperator(
        task_id='visualize_kafka_data',
        python_callable=visualize_data
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task >> visualize_task
