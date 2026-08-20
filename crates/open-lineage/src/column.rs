//! Sound column-level lineage extraction from a DataFusion [`LogicalPlan`].
//!
//! Walks the *optimized* plan bottom-up, maintaining for every node a map of
//! *output schema position -> set of physical `(dataset, column)` sources*.
//! Indexing by position (never by name) is what makes the resolution
//! scope-sound: column refs in expressions resolve to child positions via
//! `DFSchema::maybe_index_of_column`, so qualifiers participate exactly as
//! DataFusion's own scoping does and aliases/CTEs/self-joins can neither
//! collide nor fabricate datasets. Only the root node's map is published, and
//! the facet is attached to the **output** datasets, keyed by output field
//! (see `docs/open-lineage-design.md`).
//!
//! Degradation policy: any unhandled node, arity mismatch, unresolvable column
//! ref, or expression embedding a subquery drops column lineage for the whole
//! statement (`None`). A partially-correct per-column facet is
//! indistinguishable from a complete one to consumers, so whole-facet drop is
//! the only honest partial failure. There is deliberately no name-based
//! fallback. Table-level lineage is unaffected.

use std::collections::{BTreeMap, BTreeSet};

use datafusion::common::tree_node::TreeNode;
use datafusion::logical_expr::utils::grouping_set_to_exprlist;
use datafusion::logical_expr::{
    CreateMemoryTable, CreateView, DdlStatement, Distinct, Expr, JoinType, LogicalPlan, WriteOp,
};

use crate::config::OpenLineageConfig;
use crate::extract::dataset_for;
use crate::naming::DatasetName;

/// A physical source column in a real input dataset.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceColumn {
    /// The input dataset the column lives in.
    pub dataset: DatasetName,
    /// The physical column name within that dataset.
    pub column: String,
}

/// How directly a source value feeds an output column. Ordered so that merging
/// two paths to the same source keeps the strongest claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DirectKind {
    /// The source value flows through unchanged.
    Identity,
    /// The source value is transformed before reaching the output.
    Transformation,
    /// The source value is consumed by an aggregate function.
    Aggregation,
}

impl DirectKind {
    /// Returns the OpenLineage transformation-type subtype string for this kind.
    pub fn subtype(self) -> &'static str {
        match self {
            DirectKind::Identity => "IDENTITY",
            DirectKind::Transformation => "TRANSFORMATION",
            DirectKind::Aggregation => "AGGREGATION",
        }
    }
}

/// How a source influences output rows without its value flowing into them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndirectKind {
    /// The source appears in a filter predicate.
    Filter,
    /// The source appears in a join condition.
    Join,
    /// The source appears in a group-by key.
    GroupBy,
    /// The source appears in a sort key.
    Sort,
    /// The source appears in a window partition or order key.
    Window,
}

impl IndirectKind {
    /// Returns the OpenLineage transformation-type subtype string for this kind.
    pub fn subtype(self) -> &'static str {
        match self {
            IndirectKind::Filter => "FILTER",
            IndirectKind::Join => "JOIN",
            IndirectKind::GroupBy => "GROUP_BY",
            IndirectKind::Sort => "SORT",
            IndirectKind::Window => "WINDOW",
        }
    }
}

/// The direct sources of one output column.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnSources {
    /// Source -> strongest [`DirectKind`] seen on any path.
    pub direct: BTreeMap<SourceColumn, DirectKind>,
}

impl ColumnSources {
    fn single(source: SourceColumn, kind: DirectKind) -> Self {
        Self {
            direct: BTreeMap::from([(source, kind)]),
        }
    }

    fn merge(&mut self, other: &ColumnSources) {
        for (source, kind) in &other.direct {
            self.direct
                .entry(source.clone())
                .and_modify(|k| *k = (*k).max(*kind))
                .or_insert(*kind);
        }
    }

    /// All kinds upgraded to at least `min` (e.g. a column passing through an
    /// aggregate expression is an `Aggregation` source even if it entered as
    /// `Identity`).
    fn upgraded(mut self, min: DirectKind) -> Self {
        for kind in self.direct.values_mut() {
            *kind = (*kind).max(min);
        }
        self
    }
}

/// Per-source indirect influences; unions upward through the plan.
pub type IndirectSources = BTreeMap<SourceColumn, BTreeSet<IndirectKind>>;

/// Resolved column lineage for one plan node's output.
///
/// Internal to the bottom-up resolution; the crate-public result is
/// [`ResolvedColumns`].
#[derive(Debug, Clone, Default)]
pub(crate) struct NodeLineage {
    /// One entry per output schema field, positionally aligned with the node's
    /// [`DFSchema`](datafusion::common::DFSchema).
    pub columns: Vec<ColumnSources>,
    /// Influences gathered at this node and below (filter predicates, join
    /// keys, group keys, sort keys). At the root they apply to every output.
    pub indirect: IndirectSources,
}

/// Column lineage for the dataset a root `Dml`/`Ddl` plan writes: output field
/// name -> sources, plus the statement-wide indirect influences.
///
/// `None` when the plan writes nothing (pure SELECT — per spec the facet lives
/// on output datasets) or when resolution degraded.
#[derive(Debug, Clone, Default)]
pub struct ResolvedColumns {
    /// Output field name -> its direct sources.
    pub fields: BTreeMap<String, ColumnSources>,
    /// Statement-wide indirect influences, applying to every output field.
    pub indirect: IndirectSources,
}

/// Resolve the column lineage of the dataset written by `plan`.
pub(crate) fn resolve_output_columns(
    plan: &LogicalPlan,
    config: &OpenLineageConfig,
) -> Option<ResolvedColumns> {
    match plan {
        LogicalPlan::Dml(dml) => match dml.op {
            // The SQL planner aligns the DML input positionally with the
            // target table schema (one projection expr per target column);
            // key the resolved map by the target's field names. Degrade on
            // arity mismatch — non-SQL plan builders carry no such guarantee.
            WriteOp::Insert(_) | WriteOp::Ctas | WriteOp::Update => {
                let resolved = resolve(&dml.input, config)?;
                let target = dml.target.schema();
                if resolved.columns.len() != target.fields().len() {
                    return degrade(plan, "DML input not aligned with target schema");
                }
                Some(keyed_by(resolved, |i| target.field(i).name().clone()))
            }
            // No column values are written.
            WriteOp::Delete | WriteOp::Truncate => None,
        },
        // `CREATE TABLE ... AS SELECT` lowers to CreateMemoryTable; the new
        // table's fields are exactly the query's output fields. `CREATE VIEW`
        // is structurally identical (a named plan over a SELECT input).
        LogicalPlan::Ddl(
            DdlStatement::CreateMemoryTable(CreateMemoryTable { input, .. })
            | DdlStatement::CreateView(CreateView { input, .. }),
        ) => {
            let resolved = resolve(input, config)?;
            let schema = input.schema().clone();
            if resolved.columns.len() != schema.fields().len() {
                return degrade(plan, "CTAS/view input arity mismatch");
            }
            Some(keyed_by(resolved, |i| schema.field(i).name().clone()))
        }
        // CreateExternalTable has no query input; everything else writes no
        // dataset, so there is no output to attach the facet to.
        _ => None,
    }
}

fn keyed_by(node: NodeLineage, name: impl Fn(usize) -> String) -> ResolvedColumns {
    let fields = node
        .columns
        .into_iter()
        .enumerate()
        // Columns with no sources (literals, defaults filled in by the
        // planner) are omitted: an empty `inputFields` says nothing.
        .filter(|(_, sources)| !sources.direct.is_empty())
        .map(|(i, sources)| (name(i), sources))
        .collect();
    ResolvedColumns {
        fields,
        indirect: node.indirect,
    }
}

/// Strip any layers of [`Expr::Alias`] to reach the underlying expression.
///
/// `Expr::unalias` consumes `self` and clones, so this borrowing form is used
/// where only a reference to the inner expression is needed.
fn peel_aliases(expr: &Expr) -> &Expr {
    let mut inner = expr;
    while let Expr::Alias(alias) = inner {
        inner = &alias.expr;
    }
    inner
}

fn degrade<T>(plan: &LogicalPlan, reason: &str) -> Option<T> {
    tracing::debug!(
        target: "openlineage",
        node = %plan.display(),
        reason,
        "column lineage degraded"
    );
    None
}

/// Bottom-up positional resolution of a relational plan node.
fn resolve(plan: &LogicalPlan, config: &OpenLineageConfig) -> Option<NodeLineage> {
    let node = match plan {
        LogicalPlan::TableScan(scan) => {
            // information_schema is DataFusion's metadata surface, not a real
            // dataset (mirrors the table-level extraction).
            if scan
                .table_name
                .schema()
                .is_some_and(|s| s.eq_ignore_ascii_case("information_schema"))
            {
                return degrade(plan, "information_schema scan");
            }
            let dataset = dataset_for(&scan.table_name, config);
            let source_schema = scan.source.schema();
            // Map each projected output position back to the *source* schema
            // position, reporting the physical column name.
            let columns = (0..scan.projected_schema.fields().len())
                .map(|i| {
                    let src = scan.projection.as_ref().map_or(i, |p| p[i]);
                    ColumnSources::single(
                        SourceColumn {
                            dataset: dataset.clone(),
                            column: source_schema.field(src).name().clone(),
                        },
                        DirectKind::Identity,
                    )
                })
                .collect();
            // Filters pushed into the scan may reference unprojected columns,
            // so resolve them by name against the full source schema.
            let mut indirect = IndirectSources::new();
            for filter in &scan.filters {
                if has_hidden_sources(filter) {
                    return degrade(plan, "subquery in pushed-down filter");
                }
                for col in filter.column_refs() {
                    if source_schema.field_with_name(&col.name).is_err() {
                        return degrade(plan, "pushed-down filter column not in source schema");
                    }
                    indirect
                        .entry(SourceColumn {
                            dataset: dataset.clone(),
                            column: col.name.clone(),
                        })
                        .or_default()
                        .insert(IndirectKind::Filter);
                }
            }
            NodeLineage { columns, indirect }
        }

        LogicalPlan::Projection(proj) => {
            let child = resolve(&proj.input, config)?;
            let columns = proj
                .expr
                .iter()
                .map(|expr| expr_sources(expr, &proj.input, &child, DirectKind::Identity))
                .collect::<Option<_>>()?;
            NodeLineage {
                columns,
                indirect: child.indirect,
            }
        }

        // A positional re-qualification of the input: same columns, new
        // qualifier. The alias name is never consulted, so it cannot
        // fabricate a dataset.
        LogicalPlan::SubqueryAlias(alias) => resolve(&alias.input, config)?,

        LogicalPlan::Filter(filter) => {
            let mut child = resolve(&filter.input, config)?;
            let sources = referenced_sources(&filter.predicate, &filter.input, &child.columns)?;
            add_indirect(&mut child.indirect, sources, IndirectKind::Filter);
            child
        }

        LogicalPlan::Aggregate(agg) => {
            let child = resolve(&agg.input, config)?;
            let group_list = grouping_set_to_exprlist(&agg.group_expr).ok()?;
            let mut columns = Vec::with_capacity(agg.schema.fields().len());
            let mut indirect = child.indirect.clone();
            for expr in &group_list {
                let sources = expr_sources(expr, &agg.input, &child, DirectKind::Identity)?;
                // Group keys shape the output rows beyond carrying values.
                add_indirect(
                    &mut indirect,
                    sources.direct.keys().cloned().collect(),
                    IndirectKind::GroupBy,
                );
                columns.push(sources);
            }
            // Grouping sets add a synthetic `__grouping_id` field after the
            // group columns; it derives from no source value.
            if matches!(agg.group_expr.as_slice(), [Expr::GroupingSet(_)]) {
                columns.push(ColumnSources::default());
            }
            for expr in &agg.aggr_expr {
                columns.push(expr_sources(
                    expr,
                    &agg.input,
                    &child,
                    DirectKind::Aggregation,
                )?);
            }
            NodeLineage { columns, indirect }
        }

        LogicalPlan::Join(join) => {
            let left = resolve(&join.left, config)?;
            let right = resolve(&join.right, config)?;
            let mut indirect = left.indirect.clone();
            for (source, kinds) in &right.indirect {
                indirect.entry(source.clone()).or_default().extend(kinds);
            }
            for (l, r) in &join.on {
                add_indirect(
                    &mut indirect,
                    referenced_sources(l, &join.left, &left.columns)?,
                    IndirectKind::Join,
                );
                add_indirect(
                    &mut indirect,
                    referenced_sources(r, &join.right, &right.columns)?,
                    IndirectKind::Join,
                );
            }
            if let Some(filter) = &join.filter {
                // The residual filter references columns from either side.
                if has_hidden_sources(filter) {
                    return degrade(plan, "subquery in join filter");
                }
                for col in filter.column_refs() {
                    let sources = if let Some(i) = join.left.schema().maybe_index_of_column(col) {
                        &left.columns[i]
                    } else if let Some(i) = join.right.schema().maybe_index_of_column(col) {
                        &right.columns[i]
                    } else {
                        return degrade(plan, "join filter column not in either side");
                    };
                    add_indirect(
                        &mut indirect,
                        sources.direct.keys().cloned().collect(),
                        IndirectKind::Join,
                    );
                }
            }
            // The mark column a mark join appends flags subquery matches; it
            // carries no source value.
            let mark = std::iter::once(ColumnSources::default());
            let columns: Vec<ColumnSources> = match join.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    left.columns.into_iter().chain(right.columns).collect()
                }
                JoinType::LeftSemi | JoinType::LeftAnti => left.columns,
                JoinType::RightSemi | JoinType::RightAnti => right.columns,
                JoinType::LeftMark => left.columns.into_iter().chain(mark).collect(),
                JoinType::RightMark => right.columns.into_iter().chain(mark).collect(),
            };
            NodeLineage { columns, indirect }
        }

        LogicalPlan::Union(union) => {
            let mut columns = vec![ColumnSources::default(); union.schema.fields().len()];
            let mut indirect = IndirectSources::new();
            for input in &union.inputs {
                let child = resolve(input, config)?;
                if child.columns.len() != columns.len() {
                    return degrade(plan, "union input arity mismatch");
                }
                for (merged, sources) in columns.iter_mut().zip(&child.columns) {
                    merged.merge(sources);
                }
                for (source, kinds) in &child.indirect {
                    indirect.entry(source.clone()).or_default().extend(kinds);
                }
            }
            NodeLineage { columns, indirect }
        }

        LogicalPlan::Sort(sort) => {
            let mut child = resolve(&sort.input, config)?;
            for sort_expr in &sort.expr {
                let sources = referenced_sources(&sort_expr.expr, &sort.input, &child.columns)?;
                add_indirect(&mut child.indirect, sources, IndirectKind::Sort);
            }
            child
        }

        // Skip/fetch are literals; row selection by position is not a
        // column-level influence.
        LogicalPlan::Limit(limit) => resolve(&limit.input, config)?,
        LogicalPlan::Repartition(repartition) => resolve(&repartition.input, config)?,

        LogicalPlan::Distinct(Distinct::All(input)) => resolve(input, config)?,
        LogicalPlan::Distinct(Distinct::On(distinct)) => {
            let child = resolve(&distinct.input, config)?;
            let mut indirect = child.indirect.clone();
            for expr in &distinct.on_expr {
                let sources = referenced_sources(expr, &distinct.input, &child.columns)?;
                add_indirect(&mut indirect, sources, IndirectKind::GroupBy);
            }
            for sort_expr in distinct.sort_expr.iter().flatten() {
                let sources = referenced_sources(&sort_expr.expr, &distinct.input, &child.columns)?;
                add_indirect(&mut indirect, sources, IndirectKind::Sort);
            }
            let columns = distinct
                .select_expr
                .iter()
                .map(|expr| expr_sources(expr, &distinct.input, &child, DirectKind::Identity))
                .collect::<Option<_>>()?;
            NodeLineage { columns, indirect }
        }

        LogicalPlan::Window(window) => {
            let child = resolve(&window.input, config)?;
            let mut columns = child.columns.clone();
            let mut indirect = child.indirect.clone();
            for expr in &window.window_expr {
                let inner = peel_aliases(expr);
                if let Expr::WindowFunction(wf) = inner {
                    let mut sources = ColumnSources::default();
                    for arg in &wf.params.args {
                        sources.merge(&expr_sources(
                            arg,
                            &window.input,
                            &child,
                            DirectKind::Aggregation,
                        )?);
                    }
                    for key in wf
                        .params
                        .partition_by
                        .iter()
                        .chain(wf.params.order_by.iter().map(|s| &s.expr))
                    {
                        let refs = referenced_sources(key, &window.input, &child.columns)?;
                        add_indirect(&mut indirect, refs, IndirectKind::Window);
                    }
                    columns.push(sources);
                } else {
                    // Unexpected shape: treat every ref as a transformation
                    // source — coarser, still sound.
                    columns.push(expr_sources(
                        inner,
                        &window.input,
                        &child,
                        DirectKind::Transformation,
                    )?);
                }
            }
            NodeLineage { columns, indirect }
        }

        LogicalPlan::Unnest(unnest) => {
            let child = resolve(&unnest.input, config)?;
            let unnested: BTreeSet<usize> = unnest
                .list_type_columns
                .iter()
                .map(|(i, _)| *i)
                .chain(unnest.struct_type_columns.iter().copied())
                .collect();
            let columns = unnest
                .dependency_indices
                .iter()
                .map(|&dep| {
                    let sources = child.columns.get(dep)?.clone();
                    Some(if unnested.contains(&dep) {
                        sources.upgraded(DirectKind::Transformation)
                    } else {
                        sources
                    })
                })
                .collect::<Option<_>>()
                .or_else(|| degrade(plan, "unnest dependency index out of range"))?;
            NodeLineage {
                columns,
                indirect: child.indirect,
            }
        }

        // Literal rows soundly have no provenance — this is not a degradation.
        LogicalPlan::Values(values) => NodeLineage {
            columns: vec![ColumnSources::default(); values.schema.fields().len()],
            indirect: IndirectSources::new(),
        },
        LogicalPlan::EmptyRelation(empty) => NodeLineage {
            columns: vec![ColumnSources::default(); empty.schema.fields().len()],
            indirect: IndirectSources::new(),
        },

        // After optimization a surviving Subquery means decorrelation failed;
        // Extension nodes are opaque; a recursive CTE's work-table scan would
        // fabricate a dataset from the CTE's own name. Degrade rather than
        // guess.
        other => return degrade(other, "no column lineage resolution for plan node"),
    };

    // Positional indexing only works if every handler produced exactly one
    // entry per output field.
    if node.columns.len() != plan.schema().fields().len() {
        return degrade(plan, "resolved arity does not match node schema");
    }
    Some(node)
}

/// The sources of one projected expression, resolved against the child's map.
///
/// A bare column (through any aliases) copies its child sources unchanged, so
/// identity provenance survives stacked projections; any other expression
/// upgrades every referenced source to at least `Transformation`. `min` raises
/// that floor further (`Aggregation` for aggregate/window arguments).
fn expr_sources(
    expr: &Expr,
    child_plan: &LogicalPlan,
    child: &NodeLineage,
    min: DirectKind,
) -> Option<ColumnSources> {
    if has_hidden_sources(expr) {
        return degrade(child_plan, "subquery in expression");
    }
    let inner = peel_aliases(expr);
    if let Expr::Column(col) = inner {
        let i = child_plan.schema().maybe_index_of_column(col)?;
        return Some(child.columns[i].clone().upgraded(min));
    }
    let mut sources = ColumnSources::default();
    for col in inner.column_refs() {
        let i = child_plan.schema().maybe_index_of_column(col)?;
        sources.merge(&child.columns[i]);
    }
    Some(sources.upgraded(min.max(DirectKind::Transformation)))
}

/// The physical sources behind every column an expression references —
/// used for indirect influences (predicates, keys), where the expression's
/// shape doesn't matter, only which sources it touches.
fn referenced_sources(
    expr: &Expr,
    child_plan: &LogicalPlan,
    columns: &[ColumnSources],
) -> Option<Vec<SourceColumn>> {
    if has_hidden_sources(expr) {
        return degrade(child_plan, "subquery in predicate/key expression");
    }
    let mut out = Vec::new();
    for col in expr.column_refs() {
        let i = child_plan.schema().maybe_index_of_column(col)?;
        out.extend(columns[i].direct.keys().cloned());
    }
    Some(out)
}

fn add_indirect(indirect: &mut IndirectSources, sources: Vec<SourceColumn>, kind: IndirectKind) {
    for source in sources {
        indirect.entry(source).or_default().insert(kind);
    }
}

/// `column_refs` silently misses sources hidden inside subquery expressions;
/// treat their presence as unresolvable.
fn has_hidden_sources(expr: &Expr) -> bool {
    expr.exists(|e| {
        Ok(matches!(
            e,
            Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::ScalarSubquery(_)
                | Expr::OuterReferenceColumn(_, _)
        ))
    })
    .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(name: &str) -> SourceColumn {
        SourceColumn {
            dataset: DatasetName {
                namespace: "ns".into(),
                name: "t".into(),
            },
            column: name.into(),
        }
    }

    #[test]
    fn merge_keeps_strongest_kind() {
        let mut a = ColumnSources::single(source("a"), DirectKind::Identity);
        let b = ColumnSources::single(source("a"), DirectKind::Aggregation);
        a.merge(&b);
        assert_eq!(a.direct[&source("a")], DirectKind::Aggregation);
        // And merging the weaker kind back doesn't downgrade.
        let c = ColumnSources::single(source("a"), DirectKind::Identity);
        a.merge(&c);
        assert_eq!(a.direct[&source("a")], DirectKind::Aggregation);
    }

    #[test]
    fn upgrade_is_a_floor_not_an_override() {
        let s = ColumnSources::single(source("a"), DirectKind::Aggregation)
            .upgraded(DirectKind::Transformation);
        assert_eq!(s.direct[&source("a")], DirectKind::Aggregation);
        let s = ColumnSources::single(source("a"), DirectKind::Identity)
            .upgraded(DirectKind::Transformation);
        assert_eq!(s.direct[&source("a")], DirectKind::Transformation);
    }
}
