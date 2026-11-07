use serde::{Deserialize, Serialize};

/// The Firestore database used when none is named.
pub const DEFAULT_DATABASE_ID: &str = "(default)";

/// Configuration identifying a Firebase project and app.
///
/// These are public identifiers (they ship inside every web build of an
/// app); access control is enforced by Firebase Security Rules, not by
/// keeping these values secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirebaseOptions {
    /// Web API key of the project (`apiKey` in the web config).
    pub api_key: String,
    /// Project ID (`projectId`), e.g. `my-project`.
    pub project_id: String,
    /// Auth domain (`authDomain`), e.g. `my-project.firebaseapp.com`.
    pub auth_domain: Option<String>,
    /// Default Cloud Storage bucket (`storageBucket`).
    pub storage_bucket: Option<String>,
    /// App ID (`appId`).
    pub app_id: Option<String>,
    /// Firestore database ID. Defaults to `(default)`; projects may
    /// also use a named database.
    pub database_id: String,
}

impl FirebaseOptions {
    /// Creates options for a project with the default database.
    pub fn new(api_key: impl Into<String>, project_id: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            project_id: project_id.into(),
            auth_domain: None,
            storage_bucket: None,
            app_id: None,
            database_id: DEFAULT_DATABASE_ID.to_string(),
        }
    }

    /// Sets the Firestore database ID (builder style).
    pub fn with_database_id(mut self, database_id: impl Into<String>) -> Self {
        self.database_id = database_id.into();
        self
    }

    /// Sets the auth domain (builder style).
    pub fn with_auth_domain(mut self, auth_domain: impl Into<String>) -> Self {
        self.auth_domain = Some(auth_domain.into());
        self
    }

    /// Sets the storage bucket (builder style).
    pub fn with_storage_bucket(mut self, storage_bucket: impl Into<String>) -> Self {
        self.storage_bucket = Some(storage_bucket.into());
        self
    }

    /// The Firestore resource prefix for this app's database:
    /// `projects/{project_id}/databases/{database_id}`.
    pub fn database_name(&self) -> String {
        format!(
            "projects/{}/databases/{}",
            self.project_id, self.database_id
        )
    }

    /// The Firestore documents prefix:
    /// `projects/{project_id}/databases/{database_id}/documents`.
    pub fn documents_prefix(&self) -> String {
        format!("{}/documents", self.database_name())
    }
}

/// A Firebase app handle. Service clients borrow the options from an
/// `App`; cloning an `App` is cheap (options only).
#[derive(Debug, Clone)]
pub struct App {
    options: FirebaseOptions,
}

impl App {
    /// Creates an app from the given options.
    pub fn new(options: FirebaseOptions) -> Self {
        Self { options }
    }

    /// The options this app was created with.
    pub fn options(&self) -> &FirebaseOptions {
        &self.options
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_name_uses_named_database() {
        let options = FirebaseOptions::new("key", "my-project").with_database_id("mydb");
        assert_eq!(
            options.database_name(),
            "projects/my-project/databases/mydb"
        );
        assert_eq!(
            options.documents_prefix(),
            "projects/my-project/databases/mydb/documents"
        );
    }

    #[test]
    fn database_name_defaults() {
        let options = FirebaseOptions::new("key", "demo");
        assert_eq!(options.database_name(), "projects/demo/databases/(default)");
    }
}
