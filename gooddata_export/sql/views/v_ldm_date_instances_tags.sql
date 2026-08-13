-- Tags for LDM date instances (one row per (dataset_id, tag)).
--
-- Mirrors gooddata-export's v_ldm_datasets_tags but restricted to date
-- instance datasets — those that live in ldm/date_instances/*.yaml. Their
-- distinguishing trait in ldm_datasets is the absence of a data_source_id
-- (regular datasets always carry one).
--
-- Output:
--   - dataset_id: Date instance identifier (matches the YAML filename stem)
--   - tag:        One tag attached to the date instance
--
-- Dependencies (from gooddata-export):
--   - v_ldm_datasets_tags view
--   - ldm_datasets table (data_source_id column)

DROP VIEW IF EXISTS v_ldm_date_instances_tags;

CREATE VIEW v_ldm_date_instances_tags AS
SELECT
    t.dataset_id,
    t.tag
FROM v_ldm_datasets_tags AS t
JOIN ldm_datasets AS d ON d.id = t.dataset_id
WHERE d.data_source_id IS NULL OR d.data_source_id = '';
