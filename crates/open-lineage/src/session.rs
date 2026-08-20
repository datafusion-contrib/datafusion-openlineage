//! The customer-facing entry point: [`OpenLineage::builder`].
//!
//! One builder assembles the three pieces an integration needs — a
//! [`Transport`]/[`OpenLineageClient`], a [`LineageContextProvider`], and an
//! [`OpenLineageConfig`] — and installs them on a `SessionState`. The common
//! case is [`OpenLineageBuilder::from_env`] followed by
//! [`OpenLineageBuilder::instrument`].

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result as DataFusionResult;
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};

use openlineage_client::{ClientError, LineageContext, OpenLineageClient, Transport};

use crate::config::{DataFusionConfig, OpenLineageConfig};
use crate::context::{LineageContextProvider, StaticContextProvider};
use crate::rule::OpenLineageQueryPlanner;

/// Entry point for instrumenting a DataFusion session with OpenLineage.
///
/// Construct a [`OpenLineageBuilder`] with [`OpenLineage::builder`].
#[derive(Debug)]
pub struct OpenLineage;

impl OpenLineage {
    /// Start configuring OpenLineage instrumentation.
    pub fn builder() -> OpenLineageBuilder {
        OpenLineageBuilder::default()
    }
}

/// Builds and installs OpenLineage instrumentation on a [`SessionState`].
///
/// Set a transport (or client) and, optionally, a context provider and config;
/// then call [`Self::instrument`]. [`Self::from_env`] is the one-call path that
/// derives everything from the standard OpenLineage environment.
#[derive(Default)]
pub struct OpenLineageBuilder {
    client: Option<OpenLineageClient>,
    transport: Option<Arc<dyn Transport>>,
    context: Option<Arc<dyn LineageContextProvider>>,
    config: Option<OpenLineageConfig>,
}

impl std::fmt::Debug for OpenLineageBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenLineageBuilder")
            .field("has_client", &self.client.is_some())
            .field("has_transport", &self.transport.is_some())
            .field("has_context", &self.context.is_some())
            .field("config", &self.config)
            .finish()
    }
}

impl OpenLineageBuilder {
    /// Derive client, context, and config from the standard OpenLineage
    /// environment in one call.
    ///
    /// Builds the client via [`OpenLineageClient::from_env`] (reads
    /// `OPENLINEAGE_URL` / `OPENLINEAGE_ENDPOINT` / `OPENLINEAGE_API_KEY`), the
    /// config via [`OpenLineageConfig::for_datafusion_from_env`] (reads
    /// `OPENLINEAGE_NAMESPACE` / `OPENLINEAGE_TIMEOUT_MS`), and the context via
    /// [`LineageContext::from_env`] (reads the parent-run variables). Anything
    /// set explicitly on the builder beforehand is preserved.
    ///
    /// # Errors
    /// Returns [`ClientError`] if the client cannot be built — e.g. called
    /// outside a Tokio runtime, or `OPENLINEAGE_URL` is malformed.
    pub fn from_env(mut self) -> Result<Self, ClientError> {
        let config = self
            .config
            .take()
            .unwrap_or_else(OpenLineageConfig::for_datafusion_from_env);
        if self.client.is_none() && self.transport.is_none() {
            self.client = Some(OpenLineageClient::from_env()?);
        }
        if self.context.is_none() {
            let ctx = LineageContext::from_env(&config);
            self.context = Some(Arc::new(StaticContextProvider(ctx)));
        }
        self.config = Some(config);
        Ok(self)
    }

    /// Use a pre-built [`OpenLineageClient`]. Takes precedence over
    /// [`Self::transport`].
    pub fn client(mut self, client: OpenLineageClient) -> Self {
        self.client = Some(client);
        self
    }

    /// Drain events into `transport`. A client is constructed around it at
    /// [`Self::instrument`] time. Ignored if [`Self::client`] is also set.
    pub fn transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Set the per-query context provider. Defaults to an empty
    /// [`StaticContextProvider`].
    pub fn context(mut self, context: Arc<dyn LineageContextProvider>) -> Self {
        self.context = Some(context);
        self
    }

    /// Set the static config. Defaults to [`OpenLineageConfig::for_datafusion`].
    pub fn config(mut self, config: OpenLineageConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Install the instrumentation on `state`, returning the wired
    /// `SessionState`.
    ///
    /// Sets the session's query planner to an [`OpenLineageQueryPlanner`]. A
    /// pre-existing custom `QueryPlanner` is replaced; standard physical planning
    /// (including extension nodes) is preserved because the new planner delegates
    /// to a `DefaultPhysicalPlanner`.
    ///
    /// # Panics
    /// Panics if no client or transport was set and a default client must be
    /// built outside a Tokio runtime. Prefer [`Self::from_env`] (which surfaces a
    /// [`ClientError`]) or set a client built with
    /// [`OpenLineageClient::try_new`].
    pub fn instrument(self, state: SessionState) -> SessionState {
        let client = self.client.unwrap_or_else(|| match self.transport {
            Some(transport) => OpenLineageClient::new(transport),
            None => OpenLineageClient::noop(),
        });
        let context = self
            .context
            .unwrap_or_else(|| Arc::new(StaticContextProvider::default()));
        let config = self
            .config
            .unwrap_or_else(OpenLineageConfig::for_datafusion);

        let planner = Arc::new(OpenLineageQueryPlanner::new(
            client,
            context,
            config,
            Vec::new(),
        ));
        // Also stash the planner as a typed `SessionConfig` extension so the
        // `SessionContext`-level DDL path ([`OpenLineageSqlExt::sql_with_lineage`])
        // can recover it: the `QueryPlanner` trait isn't `Any`, so `query_planner()`
        // can't be downcast, but a config extension is keyed by type and retrieved
        // with `get_extension::<OpenLineageQueryPlanner>()`. One `Arc`, two roles —
        // the registered query planner and the extension are the same instance.
        let mut session_config = state.config().clone();
        session_config.set_extension(planner.clone());
        SessionStateBuilder::from(state)
            .with_config(session_config)
            .with_query_planner(planner)
            .build()
    }
}

/// Instrument `state` with an explicit client, context provider, and config.
///
/// A thin wrapper over [`OpenLineage::builder`] for callers that already hold
/// the three pieces. Prefer the builder ([`OpenLineage::builder`]) for new code.
pub fn instrument_session_state(
    state: SessionState,
    client: OpenLineageClient,
    context: Arc<dyn LineageContextProvider>,
    config: OpenLineageConfig,
) -> SessionState {
    OpenLineage::builder()
        .client(client)
        .context(context)
        .config(config)
        .instrument(state)
}

/// Instrument `state` with an empty [`StaticContextProvider`].
///
/// A thin wrapper over [`OpenLineage::builder`]. Prefer the builder for new code.
pub fn instrument_session_state_simple(
    state: SessionState,
    client: OpenLineageClient,
    config: OpenLineageConfig,
) -> SessionState {
    OpenLineage::builder()
        .client(client)
        .config(config)
        .instrument(state)
}

/// `SessionContext` ergonomics for OpenLineage instrumentation.
///
/// Two conveniences over [`OpenLineageBuilder`]:
///
/// - [`with_lineage`](Self::with_lineage) installs the instrumentation on a
///   context in one consume-and-return call, mirroring DataFusion's own
///   `SessionContext::enable_url_table(self) -> Self`. It saves the host the
///   `SessionContext::new_with_state(builder.instrument(ctx.state()))` dance. It
///   takes `impl Into<Option<OpenLineageBuilder>>`, so `ctx.with_lineage(None)`
///   uses defaults (a no-op client, default config) and
///   `ctx.with_lineage(OpenLineage::builder()…)` supplies a configured one.
/// - [`sql_with_lineage`](Self::sql_with_lineage) runs SQL *with* lineage for
///   statements DataFusion executes outside the query-planner hook — `CREATE TABLE
///   … AS SELECT` and `CREATE VIEW … AS SELECT`. `SessionContext::sql` dispatches
///   these DDL statements straight to the catalog before the instrumented
///   `QueryPlanner` ever sees the full plan, so the created dataset is silently
///   dropped from lineage (and a `CREATE VIEW` emits nothing at all). For every
///   other statement this is exactly `SessionContext::sql(...).await` and the
///   normal planner path produces the lineage.
#[async_trait]
pub trait OpenLineageSqlExt: Sized {
    /// Instrument this context with OpenLineage and return it (consume-and-return,
    /// like `SessionContext::enable_url_table`).
    ///
    /// Pass a configured [`OpenLineageBuilder`] to control the transport/client,
    /// context provider, and config — or `None` for defaults (a no-op client and
    /// [`OpenLineageConfig::for_datafusion`]). Because the argument is
    /// `impl Into<Option<OpenLineageBuilder>>`, both `ctx.with_lineage(None)` and
    /// `ctx.with_lineage(OpenLineage::builder()…)` are valid call sites. For the
    /// standard-environment path use [`try_with_lineage_from_env`](Self::try_with_lineage_from_env)
    /// (it is fallible, so it can't fold into this signature).
    fn with_lineage(self, builder: impl Into<Option<OpenLineageBuilder>>) -> Self;

    /// Instrument this context with OpenLineage, deriving everything from the
    /// environment (see [`OpenLineageBuilder::from_env`]).
    ///
    /// # Errors
    /// Returns [`ClientError`] if the client cannot be built (e.g. a malformed
    /// `OPENLINEAGE_URL`, or called outside a Tokio runtime).
    fn try_with_lineage_from_env(self) -> Result<Self, ClientError>;

    /// Run `sql`, emitting OpenLineage events even for the CTAS / `CREATE VIEW`
    /// statements DataFusion executes outside the query-planner hook. Equivalent to
    /// [`SessionContext::sql`] for every other statement.
    async fn sql_with_lineage(&self, sql: &str) -> DataFusionResult<DataFrame>;
}

#[async_trait]
impl OpenLineageSqlExt for SessionContext {
    fn with_lineage(self, builder: impl Into<Option<OpenLineageBuilder>>) -> Self {
        // Mirror `enable_url_table`: consume the context, instrument its state, and
        // rebuild. `instrument` both registers the query planner and stashes the
        // planner as a config extension, so `sql_with_lineage` can recover it. A
        // `None` builder is the empty default builder (no-op client, default cfg).
        let builder = builder.into().unwrap_or_default();
        SessionContext::new_with_state(builder.instrument(self.state()))
    }

    fn try_with_lineage_from_env(self) -> Result<Self, ClientError> {
        Ok(self.with_lineage(OpenLineage::builder().from_env()?))
    }

    async fn sql_with_lineage(&self, sql: &str) -> DataFusionResult<DataFrame> {
        let plan = self.state().create_logical_plan(sql).await?;

        // Only DDL that carries a SELECT body (CTAS / CREATE VIEW) is stolen from
        // the planner; for everything else the planner path already produces
        // lineage, so delegate to the normal execution path verbatim.
        let is_ddl_with_input = matches!(
            plan,
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(_) | DdlStatement::CreateView(_))
        );
        if !is_ddl_with_input {
            return self.execute_logical_plan(plan).await;
        }

        // An instrumented session carries the planner as a config extension; an
        // uninstrumented one does not, so this transparently degrades to plain
        // execution (no events) there.
        match self
            .state()
            .config()
            .get_extension::<OpenLineageQueryPlanner>()
        {
            Some(planner) => planner.execute_ddl_with_lineage(self, plan, sql).await,
            None => self.execute_logical_plan(plan).await,
        }
    }
}
