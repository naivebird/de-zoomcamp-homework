SELECT COUNT(*)
FROM yellow_taxi_data
WHERE lpep_dropoff_datetime >= '2019-10-01' AND lpep_dropoff_datetime < '2019-11-01'
AND trip_distance > 1 and trip_distance <= 3

SELECT
	date(lpep_pickup_datetime) as pickup_date,
	max(trip_distance) as distance
FROM yellow_taxi_data
GROUP BY pickup_date
ORDER BY distance DESC
LIMIT 1

SELECT
	z."Zone",
	-- t.lpep_pickup_datetime,
	SUM(t.total_amount) AS total
FROM yellow_taxi_data t
JOIN taxi_zone_lookup z
ON t."PULocationID" = z."LocationID"
WHERE date(t.lpep_pickup_datetime) = '2019-10-18'
GROUP BY z."Zone"
HAVING SUM(t.total_amount) >= 13000
ORDER BY total DESC

SELECT
	z."Zone" AS dropoff_zone,
	t.tip_amount
FROM yellow_taxi_data t
JOIN taxi_zone_lookup z
ON t."DOLocationID" = z."LocationID"
WHERE
	t."PULocationID" = (
		SELECT "LocationID"
		FROM taxi_zone_lookup
		WHERE "Zone" = 'East Harlem North'
	)
	AND t.lpep_pickup_datetime >= '2019-10-01' AND t.lpep_pickup_datetime < '2019-11-01'
ORDER BY tip_amount DESC