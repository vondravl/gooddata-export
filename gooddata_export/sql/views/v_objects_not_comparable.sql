-- View: v_objects_not_comparable
-- Description: Visualizations that pair a measure with a slicing
--   attribute/date the measure's fact dataset cannot reach in the LDM.
-- Purpose: CI early-warning for AFM "object is not comparable to" errors.
--
-- Background
-- ----------
-- "Object is not comparable to X" is NOT a broken reference (the targets all
-- exist). It is an LDM join-reachability failure: the AFM engine cannot find a
-- path from the measure's fact dataset to the dataset that owns the attribute /
-- date dimension placed on the visualization axis or filter. The classic case
-- is swapping a date dimension (e.g. first_seen_date -> process_date) on a
-- visualization whose metric aggregates a fact that only references the old
-- date dimension.
--
-- Approach (directed reachability over the reference graph)
-- --------------------------------------------------------
--   1. reach:   transitive closure of the LDM reference graph. Edges go
--               dataset_id -> reference_to_id (fact -> dim, dim -> dim,
--               fact -> date), from ldm_reference_sources.
--   2. measure_ds: for every (visualization, measure) the fact dataset(s) the
--               measure aggregates. Metrics resolve transitively through
--               v_metrics_datasets_ancestry (reference_type='fact'); direct
--               fact measures on the visualization resolve by splitting the
--               qualified {dataset}.{fact} id.
--   3. slice_ds: for every (visualization, axis/filter label) the dataset that
--               owns it. Resolved as attribute -> ldm_columns, specific label
--               -> ldm_labels, date label ({date}.{granularity}) -> the date
--               instance dataset.
--   4. A violation is a (measure, slice_ds) pair on a visualization where
--               NONE of the measure's fact datasets equals or reaches slice_ds.
--               The "none reach" rule (not "any") is deliberate: a metric that
--               aggregates several facts (e.g. an amount that also divides
--               by a rate from another fact) is anchored at whichever
--               fact reaches the slice, so it is comparable as long as ONE of
--               its facts reaches the slice. Flagging on the first non-reaching
--               anchor produced ~30% false positives; "none reach" is the
--               high-precision rule that still catches the date-swap class.
--
-- Service-conformed measures: some deployments compute certain measures via an
-- external service (e.g. FlexConnect) that conforms extra dimensions onto them
-- at runtime — those joins are invisible to the plain LDM graph, so such
-- measures false-positive here. This view stays policy-free and reports them;
-- consumers filter downstream using the measure_datasets column (which
-- datasets anchor the measure) against their own naming patterns.
--
-- Caveat: this is a high-precision approximation, not the full MAQL join
-- planner. Reference direction, multivalue, grain and borrowed attributes can
-- bend real comparability, so treat hits as a fast early warning to verify
-- (e.g. by executing the visualization's AFM), not as ground truth. Coverage gap on
-- the measure side: a visualization is only checkable when measure_ds can
-- anchor at least one measure to a fact dataset, so two cases go UNCHECKED
-- (false negatives, never false positives) -- a viz whose only measures are
-- inline-MAQL / derived (NULL referenced_id, no concrete metric row), and a
-- bare-id fact placed directly on a measure shelf (only qualified
-- {dataset}.{fact} direct facts resolve here). A green check is therefore
-- "no reachable-graph violation found", not "every measure proven comparable".
--
-- Dependencies (gooddata-export): ldm_reference_sources, ldm_datasets,
--   ldm_columns, ldm_labels, visualizations, visualizations_references,
--   v_metrics_datasets_ancestry.

DROP VIEW IF EXISTS v_objects_not_comparable;

CREATE VIEW v_objects_not_comparable AS
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
-- (visualization, slicing label, dataset that owns it)
slice_ds AS (
    SELECT vr.visualization_id,
           vr.workspace_id,
           vr.referenced_id AS slice_ref,
           vr.label         AS slice_label,
           vr.source        AS slice_source,
           COALESCE(
               -- Attribute id used directly as the label id (default label).
               (SELECT c.dataset_id FROM ldm_columns c
                 WHERE c.id = vr.referenced_id AND c.type = 'attribute'),
               -- Specific (non-default) label -> its attribute's dataset.
               (SELECT l.dataset_id FROM ldm_labels l
                 WHERE l.id = vr.referenced_id),
               -- Date label {date_instance}.{granularity} -> the date instance.
               (SELECT d.id FROM ldm_datasets d
                 WHERE instr(vr.referenced_id, '.') > 0
                   AND d.id = substr(vr.referenced_id, 1,
                                     instr(vr.referenced_id, '.') - 1))
           ) AS dataset_id,
           -- The owning ATTRIBUTE id (not the label) — lets consumers check
           -- the slice against AFM computeValidObjects' valid-attribute set,
           -- which is keyed by attribute id. Default label -> attribute id == referenced_id;
           -- specific label -> ldm_labels.attribute_id; date label
           -- {date}.{granularity} -> itself (date attributes use that id form).
           COALESCE(
               (SELECT c.id FROM ldm_columns c
                 WHERE c.id = vr.referenced_id AND c.type = 'attribute'),
               (SELECT l.attribute_id FROM ldm_labels l
                 WHERE l.id = vr.referenced_id),
               vr.referenced_id
           ) AS slice_attribute_id
    FROM visualizations_references vr
    -- 'rankingFilter'/'measureValueFilter' rows are the dimension those filters
    -- rank/filter over. It carries the same reachability requirement as an axis
    -- or filter attribute: a dimension the measure's fact cannot reach fails the
    -- visualization outright ("Aggregation dimension ... is not comparable to the
    -- dimensionality ... of the subtree").
    --
    -- Only the DIRECT-identifier form is taken (local_identifier IS NULL). A
    -- bucket-handle dimension is also recorded, but its attribute already has a
    -- source='attribute' row here, so admitting both would report one
    -- incompatibility twice, differing only by source -- and the bucket form is
    -- on the axis, where the pair-with-every-measure rule is the correct one.
    --
    -- A direct dimension's AfmObjectIdentifier type is a 'label' OR an
    -- 'attribute', so restricting to 'label' alone would drop half the shape this
    -- exists to catch. The filters' measure rows are object_type='metric' and
    -- dangling handles are '*_invalid', so neither slips through.
    WHERE (
            vr.object_type = 'label'
            OR (
                vr.object_type = 'attribute'
                AND vr.source IN ('rankingFilter', 'measureValueFilter')
            )
          )
      AND (
            vr.source IN ('attribute', 'filter')
            OR (
                vr.source IN ('rankingFilter', 'measureValueFilter')
                AND vr.local_identifier IS NULL
            )
          )
),
-- One row per distinct (visualization, measure) anchor set.
measures AS (
    SELECT DISTINCT visualization_id, workspace_id, measure_id
    FROM measure_ds
)
SELECT DISTINCT
    m.visualization_id,
    v.title AS visualization_title,
    m.measure_id,
    (SELECT group_concat(md.dataset_id, ', ')
       FROM (SELECT DISTINCT dataset_id FROM measure_ds md2
              WHERE md2.visualization_id = m.visualization_id
                AND md2.workspace_id = m.workspace_id
                AND md2.measure_id = m.measure_id) md
    ) AS measure_datasets,
    s.slice_ref,
    s.slice_label,
    s.slice_source,
    s.dataset_id AS slice_dataset,
    s.slice_attribute_id,
    m.workspace_id
FROM measures m
JOIN slice_ds s
    ON s.visualization_id = m.visualization_id
    AND s.workspace_id = m.workspace_id
    -- An axis attribute or attribute filter applies to the WHOLE result, so it
    -- pairs with every measure. A ranking/measure-value dimension does not: it
    -- scopes to the single measure the filter names, and AFM raises the
    -- incomparability for that subtree alone. Pairing it with every measure would
    -- manufacture a violation for any sibling measure that legitimately never
    -- touches the dimension -- the exact false-positive class the "none reach"
    -- rule above was chosen to avoid.
    -- A ranking dimension naming an object that is ALSO on the axis is the same
    -- incompatibility the axis row already reports; emitting both would differ
    -- only by source. The axis row wins: it is the broader claim (it holds for
    -- every measure, not just the ranked one). Matched on the resolved owning
    -- attribute, so a label-vs-attribute spelling difference cannot slip past.
    AND (
        s.slice_source NOT IN ('rankingFilter', 'measureValueFilter')
        OR NOT EXISTS (
            SELECT 1 FROM slice_ds s2
            WHERE s2.visualization_id = s.visualization_id
              AND s2.workspace_id = s.workspace_id
              AND s2.slice_source IN ('attribute', 'filter')
              AND s2.slice_attribute_id = s.slice_attribute_id
        )
    )
    AND (
        s.slice_source NOT IN ('rankingFilter', 'measureValueFilter')
        OR m.measure_id = (
            -- The filter's ranked measure, resolved handle -> catalog id. Scoped
            -- only when the visualization has exactly ONE filter of that type, so
            -- the handle is unambiguous; otherwise this yields NULL and the row is
            -- dropped -- a false negative, which is the direction this view errs in.
            SELECT mr.referenced_id
            FROM visualizations_filters vf
            JOIN visualizations_references mr
              ON mr.visualization_id = vf.visualization_id
             AND mr.workspace_id = vf.workspace_id
             AND mr.source = 'measure'
             AND mr.local_identifier = vf.measure_local_identifier
            WHERE vf.visualization_id = s.visualization_id
              AND vf.workspace_id = s.workspace_id
              AND vf.filter_type = s.slice_source
              AND (
                  SELECT COUNT(*) FROM visualizations_filters vf2
                  WHERE vf2.visualization_id = s.visualization_id
                    AND vf2.workspace_id = s.workspace_id
                    AND vf2.filter_type = s.slice_source
              ) = 1
        )
    )
LEFT JOIN visualizations v
    ON v.visualization_id = m.visualization_id
    AND v.workspace_id = m.workspace_id
WHERE s.dataset_id IS NOT NULL
  -- The measure is NOT comparable with the slice when none of its fact
  -- datasets equals or reaches the slice dataset.
  AND NOT EXISTS (
      SELECT 1 FROM measure_ds md
      WHERE md.visualization_id = m.visualization_id
        AND md.workspace_id = m.workspace_id
        AND md.measure_id = m.measure_id
        AND (
            md.dataset_id = s.dataset_id
            OR EXISTS (
                SELECT 1 FROM reach r
                WHERE r.src = md.dataset_id AND r.dst = s.dataset_id
            )
        )
  )
ORDER BY m.visualization_id, m.measure_id, s.slice_ref;
