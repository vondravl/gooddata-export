-- Create view showing per-dashboard filter configuration with visibility.
--
-- Each row is one filter config on a dashboard (or tab). The key column is
-- `filter_visible`, derived from `mode`: a filter is hidden only when its mode
-- is explicitly 'hidden'; a missing mode means the platform default (visible).
--
-- This is what dashboards_references can't answer: references record that a
-- filter is present, but not whether it is shown. Use this view to check, e.g.,
-- whether a given filter is present AND visible on every dashboard.
--
-- The filter's title/selection live in its filter context, joined by
-- local_identifier. Scalar subqueries are used (instead of a JOIN) so a row
-- can never fan out if a local_identifier recurs across filter contexts. The
-- match is constrained by filter_type (so a 'date' row never picks up an
-- 'attribute' filter's title) and skips the empty local_identifier of the
-- common date filter (which would otherwise match any unkeyed filter).

DROP VIEW IF EXISTS v_dashboards_filters;

CREATE VIEW v_dashboards_filters AS
SELECT
    df.dashboard_id,
    d.title AS dashboard_title,
    df.workspace_id,
    df.tab_id,
    df.local_identifier,
    df.filter_type,
    df.mode,
    CASE WHEN df.mode = 'hidden' THEN 0 ELSE 1 END AS filter_visible,
    df.display_as_label_id,
    df.date_dataset_id,
    (
        SELECT fcf.title
        FROM filter_context_fields fcf
        WHERE fcf.workspace_id = df.workspace_id
          AND fcf.local_identifier = df.local_identifier
          AND df.local_identifier <> ''
          AND fcf.filter_type = CASE df.filter_type
                                    WHEN 'date' THEN 'dateFilter'
                                    ELSE 'attributeFilter'
                                END
        LIMIT 1
    ) AS filter_title,
    (
        SELECT fcf.display_form_id
        FROM filter_context_fields fcf
        WHERE fcf.workspace_id = df.workspace_id
          AND fcf.local_identifier = df.local_identifier
          AND df.local_identifier <> ''
          AND fcf.filter_type = CASE df.filter_type
                                    WHEN 'date' THEN 'dateFilter'
                                    ELSE 'attributeFilter'
                                END
        LIMIT 1
    ) AS display_form_id
FROM dashboards_filters df
LEFT JOIN dashboards d
    ON df.dashboard_id = d.dashboard_id
   AND df.workspace_id = d.workspace_id
ORDER BY df.dashboard_id, df.tab_id, df.filter_type, df.local_identifier;
