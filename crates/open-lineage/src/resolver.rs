//! Pluggable dataset identity resolution for DataFusion table sources.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::{TableProvider, source_as_provider};
use datafusion::logical_expr::{DdlStatement, LogicalPlan, TableSource, WriteOp};
use datafusion::sql::TableReference;

use crate::DatasetName;
use crate::facets::SymlinkIdentifier;

/// Whether a logical-plan node reads or writes a dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetAccess {
    /// A table scan reads the dataset.
    Read,
    /// A DML or DDL statement writes the dataset.
    Write,
}

/// A logical source contributing to a resolved dataset.
///
/// Sources are retained for facet factories to inspect provider metadata. DDL
/// targets have no source; distinct sources can share a canonical identity.
#[derive(Clone)]
pub struct DatasetOrigin {
    /// The logical table reference before identity resolution.
    pub table_ref: TableReference,
    /// The scan source or DML target, absent for DDL targets.
    pub source: Option<Arc<dyn TableSource>>,
}

impl Debug for DatasetOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetOrigin")
            .field("table_ref", &self.table_ref)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl DatasetOrigin {
    /// Unwrap DataFusion's default table source, if present.
    pub fn table_provider(&self) -> Option<Arc<dyn TableProvider>> {
        self.source
            .as_ref()
            .and_then(|source| source_as_provider(source).ok())
    }
}

/// A canonical dataset and the logical sources that resolved to it.
#[derive(Debug, Clone)]
pub struct ResolvedDataset {
    /// Canonical identity used in emitted events and column lineage.
    pub name: DatasetName,
    /// Input or output role. Reading and writing one identity are distinct.
    pub access: DatasetAccess,
    /// Distinct table-reference/source pairs contributing to this dataset.
    pub origins: Vec<DatasetOrigin>,
}

/// Information available when resolving a dataset's OpenLineage identity.
pub struct DatasetResolutionContext<'a> {
    /// The table reference carried by the logical-plan node.
    pub table_ref: &'a TableReference,
    /// The scan source or DML target; absent for DDL targets.
    pub source: Option<&'a Arc<dyn TableSource>>,
    /// Whether this occurrence reads or writes the dataset.
    pub access: DatasetAccess,
    /// The configured job namespace, used when no resolver returns a name.
    pub default_namespace: &'a str,
}

impl DatasetResolutionContext<'_> {
    /// Get the provider wrapped by DataFusion's default table source.
    ///
    /// Returns `None` for absent or custom table sources. Resolvers can inspect
    /// [`Self::source`] directly to handle other source implementations.
    pub fn table_provider(&self) -> Option<Arc<dyn TableProvider>> {
        self.source
            .and_then(|source| source_as_provider(source).ok())
    }
}

/// Resolves a logical table reference to its canonical OpenLineage identity.
///
/// Resolvers run in registration order until one returns `Some`. Return `None`
/// for an unrecognized source or unavailable identity; if every resolver returns
/// `None`, extraction uses the table reference under the configured job namespace.
///
/// Resolution runs before START and may await metadata I/O. Implementations must
/// bound that I/O, log lookup failures and return `None` rather than fail the
/// query. Resolution should inspect metadata without preparing or executing
/// writes. The host owns identities that become available later in execution.
#[async_trait]
pub trait DatasetResolver: Debug + Send + Sync {
    /// Return a canonical name, or `None` to try the next resolver.
    async fn resolve(&self, context: &DatasetResolutionContext<'_>) -> Option<DatasetName>;

    /// Return alternate identifiers for the dataset resolved by this resolver.
    ///
    /// Called only after this resolver returns `Some` from [`Self::resolve`],
    /// with the same context, once per distinct request within an extraction.
    /// The default returns no symlinks. Return an empty vector when unavailable;
    /// any metadata I/O must be bounded just as for [`Self::resolve`].
    ///
    /// Nonempty results become the `symlinks` dataset facet. Aliases from
    /// requests with the same canonical identity and access mode are merged,
    /// removing duplicate namespace/name/type triples. They do not change the
    /// canonical identity used by dataset or column lineage.
    async fn symlinks(&self, _context: &DatasetResolutionContext<'_>) -> Vec<SymlinkIdentifier> {
        Vec::new()
    }
}

struct DatasetRequest {
    table_ref: TableReference,
    source: Option<Arc<dyn TableSource>>,
    access: DatasetAccess,
}

impl DatasetRequest {
    fn from_plan(plan: &LogicalPlan) -> Option<Self> {
        let (table_ref, source, access) = match plan {
            LogicalPlan::TableScan(scan) if !is_information_schema(&scan.table_name) => {
                (&scan.table_name, Some(&scan.source), DatasetAccess::Read)
            }
            LogicalPlan::Dml(dml) if dml.op != WriteOp::Truncate => {
                (&dml.table_name, Some(&dml.target), DatasetAccess::Write)
            }
            LogicalPlan::Ddl(ddl) => {
                let name = match ddl {
                    DdlStatement::CreateExternalTable(cmd) => &cmd.name,
                    DdlStatement::CreateMemoryTable(cmd) => &cmd.name,
                    DdlStatement::CreateView(cmd) => &cmd.name,
                    _ => return None,
                };
                (name, None, DatasetAccess::Write)
            }
            _ => return None,
        };
        Some(Self {
            table_ref: table_ref.clone(),
            source: source.cloned(),
            access,
        })
    }

    fn matches(
        &self,
        table_ref: &TableReference,
        source: Option<&Arc<dyn TableSource>>,
        access: DatasetAccess,
    ) -> bool {
        self.table_ref == *table_ref
            && self.access == access
            && match (&self.source, source) {
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                (None, None) => true,
                _ => false,
            }
    }
}

struct ResolvedIdentity {
    name: DatasetName,
    symlinks: Vec<SymlinkIdentifier>,
}

/// A query-local lookup shared by table and column extraction. Source identity
/// matters: unrelated scans can carry the same logical table reference.
pub(crate) struct DatasetNames<'a> {
    default_namespace: &'a str,
    resolved: Vec<(DatasetRequest, ResolvedIdentity)>,
}

impl<'a> DatasetNames<'a> {
    pub(crate) fn new(default_namespace: &'a str) -> Self {
        Self {
            default_namespace,
            resolved: Vec::new(),
        }
    }

    pub(crate) async fn resolve(
        &mut self,
        plan: &LogicalPlan,
        resolvers: &[Arc<dyn DatasetResolver>],
    ) {
        let mut requests = Vec::new();
        // This walk never errors. Keep the visitors synchronous and await only
        // the metadata lookups, once per distinct request (including fallback).
        let _ = plan.apply(|node| {
            if let Some(request) = DatasetRequest::from_plan(node) {
                requests.push(request);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        for request in requests {
            if self.resolved.iter().any(|(seen, _)| {
                seen.matches(&request.table_ref, request.source.as_ref(), request.access)
            }) {
                continue;
            }
            let context = DatasetResolutionContext {
                table_ref: &request.table_ref,
                source: request.source.as_ref(),
                access: request.access,
                default_namespace: self.default_namespace,
            };
            let mut name = None;
            let mut symlinks = Vec::new();
            for resolver in resolvers {
                name = resolver.resolve(&context).await;
                if name.is_some() {
                    symlinks = resolver.symlinks(&context).await;
                    break;
                }
            }
            let name = name.unwrap_or_else(|| {
                DatasetName::from_table_ref(self.default_namespace, &request.table_ref.to_string())
            });
            self.resolved
                .push((request, ResolvedIdentity { name, symlinks }));
        }
    }

    pub(crate) fn get(
        &self,
        table_ref: &TableReference,
        source: Option<&Arc<dyn TableSource>>,
        access: DatasetAccess,
    ) -> DatasetName {
        self.resolved
            .iter()
            .find(|(request, _)| request.matches(table_ref, source, access))
            .map(|(_, dataset)| dataset.name.clone())
            .unwrap_or_else(|| {
                DatasetName::from_table_ref(self.default_namespace, &table_ref.to_string())
            })
    }

    pub(crate) fn datasets(&self) -> Vec<ResolvedDataset> {
        let mut datasets: Vec<ResolvedDataset> = Vec::new();
        for (request, identity) in &self.resolved {
            let origin = DatasetOrigin {
                table_ref: request.table_ref.clone(),
                source: request.source.clone(),
            };
            if let Some(dataset) = datasets
                .iter_mut()
                .find(|dataset| dataset.name == identity.name && dataset.access == request.access)
            {
                dataset.origins.push(origin);
            } else {
                datasets.push(ResolvedDataset {
                    name: identity.name.clone(),
                    access: request.access,
                    origins: vec![origin],
                });
            }
        }
        datasets
    }

    pub(crate) fn symlinks(
        &self,
        name: &DatasetName,
        access: DatasetAccess,
    ) -> Vec<SymlinkIdentifier> {
        let mut symlinks = Vec::new();
        for (request, dataset) in &self.resolved {
            if request.access == access && dataset.name == *name {
                for alias in &dataset.symlinks {
                    if !symlinks.contains(alias) {
                        symlinks.push(alias.clone());
                    }
                }
            }
        }
        symlinks
    }
}

pub(crate) fn is_information_schema(table_ref: &TableReference) -> bool {
    table_ref
        .schema()
        .is_some_and(|schema| schema.eq_ignore_ascii_case("information_schema"))
}
