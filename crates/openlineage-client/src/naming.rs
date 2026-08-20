//! OpenLineage dataset naming-spec mapping.
//!
//! Conformance here is what lets lineage graphs join across tools. See
//! <https://openlineage.io/docs/spec/naming>.

use url::Url;

/// An OpenLineage dataset name: a `namespace` plus a `name` unique within it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatasetName {
    /// The dataset's namespace (e.g. a storage root or job namespace).
    pub namespace: String,
    /// The dataset's name, unique within its namespace.
    pub name: String,
}

impl DatasetName {
    /// Map a storage location URL to its OpenLineage `(namespace, name)`.
    ///
    /// - `s3://bucket/key...` -> namespace `s3://bucket`, name `key...`
    /// - `gs://bucket/key...` -> namespace `gs://bucket`, name `key...`
    /// - `file:///path`       -> namespace `file`, name `/path`
    /// - anything else        -> namespace `{scheme}://{host}`, name `{path}`
    pub fn from_location(url: &Url) -> Self {
        let scheme = url.scheme();
        match scheme {
            "s3" | "s3a" | "gs" | "gcs" | "abfs" | "abfss" | "wasbs" => {
                let bucket = url.host_str().unwrap_or_default();
                let name = url.path().trim_start_matches('/').to_string();
                DatasetName {
                    namespace: format!("{scheme}://{bucket}"),
                    name,
                }
            }
            "file" => DatasetName {
                namespace: "file".to_string(),
                name: url.path().to_string(),
            },
            _ => {
                let host = url.host_str().unwrap_or_default();
                DatasetName {
                    namespace: format!("{scheme}://{host}"),
                    name: url.path().trim_start_matches('/').to_string(),
                }
            }
        }
    }

    /// A fallback name for a table reference that has no resolvable location
    /// (e.g. an in-memory or unqualified table). Uses the configured job
    /// namespace so it still lands somewhere sensible.
    pub fn from_table_ref(default_namespace: &str, table_ref: &str) -> Self {
        DatasetName {
            namespace: default_namespace.to_string(),
            name: table_ref.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_location() {
        let url = Url::parse("s3://open-lakehouse/warehouse/t1").unwrap();
        let n = DatasetName::from_location(&url);
        assert_eq!(n.namespace, "s3://open-lakehouse");
        assert_eq!(n.name, "warehouse/t1");
    }

    #[test]
    fn file_location() {
        let url = Url::parse("file:///tmp/data/t").unwrap();
        let n = DatasetName::from_location(&url);
        assert_eq!(n.namespace, "file");
        assert_eq!(n.name, "/tmp/data/t");
    }
}
