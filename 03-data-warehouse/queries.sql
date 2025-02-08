
CREATE OR REPLACE EXTERNAL TABLE `yellow_trip_data_2024.yellow_trip_data`
OPTIONS (
  format = 'parquet',
  uris = ['gs://de-zoomcamp-448805/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `yellow_trip_data_2024.yellow_trip_data_nonpartitioned`
AS SELECT * FROM `yellow_trip_data_2024.yellow_trip_data`;

SELECT COUNT(DISTINCT(PULocationID)) FROM `yellow_trip_data_2024.yellow_trip_data`;
SELECT COUNT(DISTINCT(PULocationID)) FROM `yellow_trip_data_2024.yellow_trip_data_nonpartitioned`;

SELECT PULocationID
FROM `yellow_trip_data_2024.yellow_trip_data_nonpartitioned`;


SELECT PULocationID, DOLocationID
FROM `yellow_trip_data_2024.yellow_trip_data_nonpartitioned`;

SELECT COUNT(*)
FROM `yellow_trip_data_2024.yellow_trip_data_nonpartitioned`
WHERE fare_amount = 0;

CREATE OR REPLACE TABLE `yellow_trip_data_2024.yellow_trip_data_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `yellow_trip_data_2024.yellow_trip_data`
);

SELECT DISTINCT VendorID
FROM `yellow_trip_data_2024.yellow_trip_data_nonpartitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM `yellow_trip_data_2024.yellow_trip_data_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
