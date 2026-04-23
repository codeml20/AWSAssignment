SELECT
    year,
    COUNT(*) AS total_records
FROM cleaned_partitioned
GROUP BY year
ORDER BY year;