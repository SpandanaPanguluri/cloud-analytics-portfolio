-- Hospital Quality KPIs
-- Cloud ready SQL Compatible with Athena / Snowflake / Synapse Serverless

-- KPI 1: Rating summary by state
SELECT
    state,
    COUNT(*) AS hospital_count,
    AVG(hospital_overall_rating) AS avg_rating,
    SUM(CASE WHEN hospital_overall_rating IS NULL THEN 1 ELSE 0 END) AS missing_ratings
FROM hospital_general_information
GROUP BY state
ORDER BY avg_rating DESC;

-- KPI 2: Hospital type breakdown
SELECT
  hospital_type,
  COUNT(*) AS hospital_count,
  AVG(CAST(hospital_overall_rating AS DOUBLE)) AS avg_rating
FROM hospital_general_information
GROUP BY hospital_type
ORDER BY hospital_count DESC;

-- KPI 3: Lowest-rated hospitals (quality improvement targets)
SELECT
  hospital_name,
  city,
  state,
  hospital_type,
  CAST(hospital_overall_rating AS DOUBLE) AS hospital_overall_rating
FROM hospital_general_information
WHERE hospital_overall_rating IS NOT NULL
ORDER BY hospital_overall_rating ASC, hospital_name
LIMIT 25;

-- KPI 4: Missing ratings list (data quality)
SELECT
  hospital_name,
  city,
  state,
  hospital_type
FROM hospital_general_information
WHERE hospital_overall_rating IS NULL
ORDER BY state, hospital_name;
