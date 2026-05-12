CREATE OR REPLACE VIEW job_requirement_items AS

SELECT
    source,
    source_job_id,
    'technical_tool' AS item_type,
    item AS item_value
FROM job_requirements,
UNNEST(technical_tools) AS t(item)

UNION ALL

SELECT
    source,
    source_job_id,
    'technical_concept' AS item_type,
    item AS item_value
FROM job_requirements,
UNNEST(technical_concepts) AS t(item)

UNION ALL

SELECT
    source,
    source_job_id,
    'certification' AS item_type,
    item AS item_value
FROM job_requirements,
UNNEST(certifications) AS t(item);