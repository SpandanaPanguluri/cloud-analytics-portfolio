-- Hospital Quality KPIs
-- Compatible with Athena / Snowflake / Synapse Serverless

SELECT
    state,
    COUNT(*) AS hospital_count,
    AVG(hospital_overall_rating) AS avg_rating,
    SUM(CASE WHEN hospital_overall_rating IS NULL THEN 1 ELSE 0 END) AS missing_ratings
FROM hospital_general_information
GROUP BY state
ORDER BY avg_rating DESC;
