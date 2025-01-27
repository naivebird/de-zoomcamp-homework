FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py" ]

# docker run --network 01-docker-terraform_pg-network ingest-ny-taxi-data:latest --user root --password root --host pgdatabase --port 5432 --db ny_taxi --table_name yellow_taxi_data --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz