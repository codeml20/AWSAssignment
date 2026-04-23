SELECT
    year,
    SUM(CAST(value AS DOUBLE)) AS total_value
FROM cleaned_partitioned
GROUP BY year
ORDER BY year;