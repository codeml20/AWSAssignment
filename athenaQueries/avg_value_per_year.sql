SELECT
    year,
    AVG(CAST(value AS DOUBLE)) AS avg_value
FROM cleaned_partitioned
GROUP BY year
ORDER BY year;