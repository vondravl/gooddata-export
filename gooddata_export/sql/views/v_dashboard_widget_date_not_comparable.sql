-- View: v_dashboard_widget_date_not_comparable
-- Description: Dashboard tiles bound to a date dimension (widget.dateDataSet)
--   that the tile insight's measures cannot reach in the LDM.
-- Purpose: CI early-warning for the "this date can no longer be applied to the
--   visualization" / AFM "object is not comparable to" class, at the
--   DASHBOARD-WIDGET layer.
--
-- Why this is separate from v_objects_not_comparable
-- --------------------------------------------------------
-- objects_not_comparable inspects slices/filters that live INSIDE the insight
-- object (visualizations_references). It cannot see a date dimension that is
-- bound by the DASHBOARD WIDGET via `widget.dateDataSet` — a headline KPI with
-- `filters: []` and no date axis is comparable in isolation, yet becomes
-- non-comparable the moment a tile binds e.g. `first_seen_date` to it while the
-- insight's measures aggregate a fact that only references `process_date`.
-- GoodData raises this in the dashboard tile config, not the insight editor.
-- This view models exactly that layer.
--
-- The binding is already exported: `dashboards_widget_filters` carries one row
-- per widget (and per visualizationSwitcher child) with
-- filter_type='dateDataSet' and reference_id = the bound date-dataset id.
--
-- Approach: identical reachability to v_objects_not_comparable, but the
-- "slice" is the widget's date dataset rather than an insight-internal label.
--   1. reach:      transitive closure of the LDM reference graph
--                  (fact -> dim, dim -> dim, fact -> date), from
--                  ldm_reference_sources. Date instances ARE valid reach
--                  targets (facts edge directly to their date datasets).
--   2. measure_ds: for every (visualization, measure) the fact dataset(s) the
--                  measure aggregates (metrics via v_metrics_datasets_ancestry;
--                  direct {dataset}.{fact} measures by id split). Same as the
--                  sibling view.
--   3. widget_date: every dashboard widget's bound date dataset.
--   4. A violation is a (widget, measure) where NONE of the measure's fact
--      datasets equals or reaches the widget's date dataset.
--
-- Service-conformed measures: UNLIKE the sibling view's downstream policy, no
-- exclusion applies here even for measures computed via an external service.
-- Such services conform extra *attribute* dimensions onto measures; date
-- dimensions keep their plain LDM relationships, so date reachability stays
-- accurate for every measure and a hit is a genuine comparability failure the
-- date filter would silently drop.
--
-- Caveat: high-precision reachability approximation, not the full MAQL join
-- planner — treat hits as a fast early warning to confirm by executing the
-- tile's real AFM under its date dataset, not as ground truth. Same measure-side coverage gap as the sibling view:
-- inline-MAQL / derived measures with no concrete fact row go UNCHECKED
-- (false negatives, never false positives).
--
-- Dependencies (gooddata-export): dashboards_widget_filters, dashboards,
--   ldm_reference_sources, visualizations, visualizations_references,
--   dashboards_visualizations, v_metrics_datasets_ancestry.

DROP VIEW IF EXISTS v_dashboard_widget_date_not_comparable;

CREATE VIEW v_dashboard_widget_date_not_comparable AS
WITH RECURSIVE reach(src, dst) AS (
    -- Direct edges: a dataset reaches every dataset/date it references.
    SELECT DISTINCT dataset_id, reference_to_id
    FROM ldm_reference_sources
    UNION
    -- Transitive: follow outgoing references of already-reached datasets.
    SELECT r.src, rs.reference_to_id
    FROM reach r
    JOIN ldm_reference_sources rs ON rs.dataset_id = r.dst
),
-- (visualization, measure, fact dataset the measure aggregates)
measure_ds AS (
    -- Metrics on the visualization -> their fact datasets (transitive).
    SELECT vr.visualization_id,
           vr.workspace_id,
           vr.referenced_id AS measure_id,
           mda.dataset_id   AS dataset_id
    FROM visualizations_references vr
    JOIN v_metrics_datasets_ancestry mda
        ON mda.metric_id = vr.referenced_id
        AND mda.workspace_id = vr.workspace_id
        AND mda.reference_type = 'fact'
    WHERE vr.object_type = 'metric'
    UNION
    -- Direct fact measures: referenced_id is a qualified {dataset}.{fact} id.
    SELECT vr.visualization_id,
           vr.workspace_id,
           vr.referenced_id AS measure_id,
           substr(vr.referenced_id, 1, instr(vr.referenced_id, '.') - 1) AS dataset_id
    FROM visualizations_references vr
    WHERE vr.object_type = 'fact'
      AND instr(vr.referenced_id, '.') > 0
),
-- Every dashboard widget's bound date dataset (incl. switcher children).
widget_date AS (
    SELECT dashboard_id,
           visualization_id,
           widget_local_identifier,
           reference_id AS date_dataset,
           workspace_id
    FROM dashboards_widget_filters
    WHERE filter_type = 'dateDataSet'
      AND reference_id IS NOT NULL
),
-- One row per distinct (visualization, measure) anchor set.
measures AS (
    SELECT DISTINCT visualization_id, workspace_id, measure_id
    FROM measure_ds
)
SELECT DISTINCT
    wd.dashboard_id,
    d.title AS dashboard_title,
    wd.widget_local_identifier,
    dv.widget_title,
    m.visualization_id,
    v.title AS visualization_title,
    m.measure_id,
    (SELECT group_concat(md.dataset_id, ', ')
       FROM (SELECT DISTINCT dataset_id FROM measure_ds md2
              WHERE md2.visualization_id = m.visualization_id
                AND md2.workspace_id = m.workspace_id
                AND md2.measure_id = m.measure_id) md
    ) AS measure_datasets,
    wd.date_dataset,
    m.workspace_id
FROM widget_date wd
JOIN measures m
    ON m.visualization_id = wd.visualization_id
    AND m.workspace_id = wd.workspace_id
LEFT JOIN dashboards d
    ON d.dashboard_id = wd.dashboard_id
    AND d.workspace_id = wd.workspace_id
LEFT JOIN dashboards_visualizations dv
    ON dv.dashboard_id = wd.dashboard_id
    AND dv.visualization_id = wd.visualization_id
    AND dv.widget_local_identifier = wd.widget_local_identifier
    AND dv.workspace_id = wd.workspace_id
LEFT JOIN visualizations v
    ON v.visualization_id = m.visualization_id
    AND v.workspace_id = m.workspace_id
-- The measure is NOT comparable with the widget's date dataset when none of
-- its fact datasets equals or reaches it. (Service-computed measures are
-- checked too — date dims are not service-rewritten; see note above.)
WHERE NOT EXISTS (
      SELECT 1 FROM measure_ds md
      WHERE md.visualization_id = m.visualization_id
        AND md.workspace_id = m.workspace_id
        AND md.measure_id = m.measure_id
        AND (
            md.dataset_id = wd.date_dataset
            OR EXISTS (
                SELECT 1 FROM reach r
                WHERE r.src = md.dataset_id AND r.dst = wd.date_dataset
            )
        )
  )
ORDER BY wd.dashboard_id, m.visualization_id, m.measure_id;
