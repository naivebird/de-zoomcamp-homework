import csv
import gzip
import json
from time import time

from kafka import KafkaProducer

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    csv_file = 'data/green_tripdata_2019-10.csv.gz'  # change to your CSV file path if needed

    with gzip.open(csv_file, 'rt', newline='') as file:
        reader = csv.DictReader(file)
        start = time()
        for row in reader:
            # Each row will be a dictionary keyed by the CSV headers
            # Send data to Kafka topic "green-data"
            message = dict(
                lpep_pickup_datetime=row['lpep_pickup_datetime'],
                lpep_dropoff_datetime=row['lpep_dropoff_datetime'],
                PULocationID=row['PULocationID'],
                DOLocationID=row['DOLocationID'],
                # passenger_count=row['passenger_count'],
                trip_distance=row['trip_distance'],
                tip_amount=row['tip_amount']
            )
            producer.send('green-trips', value=message)
            print("Sent: ", message)

    # Make sure any remaining messages are delivered
    producer.flush()
    end = time()
    print("Total time: ", end - start)
    producer.close()


if __name__ == "__main__":
    main()
