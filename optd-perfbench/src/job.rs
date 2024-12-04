// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

use csv2parquet::Opts;
use datafusion::catalog::SchemaProvider;
/// A wrapper around job-kit
use serde::{Deserialize, Serialize};

use crate::shell;

const JOB_KIT_REPO_URL: &str = "https://github.com/wangpatrick57/job-kit.git";
const JOB_TABLES_URL: &str = "https://homepages.cwi.nl/~boncz/job/imdb.tgz";
pub const WORKING_JOB_QUERY_IDS: &[&str] = &[
    "1a", "1b", "1c", "1d", "2a", "2b", "2d", "3a", "3b", "3c", "4a", "4b", "4c", "5c", "6a", "6b",
    "6c", "6d", "6e", "6f", "7b", "8a", "8b", "8c", "8d", "9b", "9c", "9d", "10a", "10c", "12a",
    "12b", "12c", "13a", "13b", "13c", "13d", "14a", "14b", "14c", "15a", "15b", "15c", "15d",
    "16a", "16b", "16c", "16d", "17a", "17b", "17c", "17d", "17e", "17f", "18a", "18c", "19b",
    "19c", "19d", "20a", "20b", "20c", "22a", "22b", "22c", "22d", "23a", "23b", "23c", "24a",
    "24b", "25a", "25b", "25c", "26a", "26b", "26c", "28a", "28b", "28c", "29a", "29b", "29c",
    "30a", "30b", "30c", "31a", "31b", "31c", "32b", "33a", "33b", "33c",
];
pub const WORKING_JOBLIGHT_QUERY_IDS: &[&str] = &[
    "1a", "1b", "1c", "1d", "2a", "3a", "3b", "4a", "4b", "4c", "5a", "5b", "5c", "6a", "6b", "6c",
    "6d", "7a", "7b", "7c", "8a", "8b", "8c", "9a", "9b", "10a", "10b", "10c", "11a", "11b", "11c",
    "12a", "12b", "12c", "13a", "14a", "14b", "14c", "16a", "17a", "17b", "17c", "18a", "19b",
    "20a", "20b", "20c", "21a", "21b", "22b", "23b", "24a", "24b", "25a", "26a", "26b", "27a",
    "27b",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobKitConfig {
    pub query_ids: Vec<String>,
    pub is_light: bool,
}

impl Display for JobKitConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Use write! macro to write formatted string to `f`
        write!(f, "JobKitConfig(query_ids={:?})", self.query_ids,)
    }
}

/// Provides many helper functions for running a JOB workload.
/// It does not actually execute the queries as it is meant to be DBMS-agnostic.
/// Is essentially a wrapper around the job-kit repo.
/// Since it's conceptually a wrapper around the repo, I chose _not_ to make
///   JobKitConfig an initialization parameter.
pub struct JobKit {
    _workspace_dpath: PathBuf,

    // cache these paths so we don't have to build them multiple times
    job_dpath: PathBuf,
    job_kit_repo_dpath: PathBuf,
    downloaded_tables_dpath: PathBuf,
    job_queries_dpath: PathBuf,
    joblight_queries_dpath: PathBuf,
    pub schema_fpath: PathBuf,
    pub indexes_fpath: PathBuf,
}

impl JobKit {
    pub fn build<P: AsRef<Path>>(workspace_dpath: P) -> io::Result<Self> {
        log::debug!("[start] building JobKit");

        // Build paths, sometimes creating them if they don't exist
        let workspace_dpath = workspace_dpath.as_ref().to_path_buf();
        let job_dpath = workspace_dpath.join("job");
        if !job_dpath.exists() {
            fs::create_dir(&job_dpath)?;
        }
        let job_kit_repo_dpath = job_dpath.join("job-kit");
        let job_queries_dpath = job_kit_repo_dpath.join("cardest_job_queries");
        let joblight_queries_dpath = job_kit_repo_dpath.join("cardest_joblight_queries");
        let downloaded_tables_dpath = job_dpath.join("downloaded_tables");
        if !downloaded_tables_dpath.exists() {
            fs::create_dir(&downloaded_tables_dpath)?;
        }
        // Note that the downloaded tables directory has a file called schematext.sql.
        // I chose to use the schema.sql in the repo itself for one simple reason: since
        //   we forked the repo, we can modify the schema.sql if necessary.
        // Note also that I copied schematext.sql into our job-kit fork *for reference only*.
        let schema_fpath = job_kit_repo_dpath.join("schema.sql");
        let indexes_fpath = job_kit_repo_dpath.join("fkindexes.sql");

        // Create Self
        let kit = JobKit {
            _workspace_dpath: workspace_dpath,
            job_dpath,
            job_kit_repo_dpath,
            job_queries_dpath,
            joblight_queries_dpath,
            downloaded_tables_dpath,
            schema_fpath,
            indexes_fpath,
        };

        // setup
        shell::clonepull_repo(JOB_KIT_REPO_URL, &kit.job_kit_repo_dpath)?;

        log::debug!("[end] building TpchKit");
        Ok(kit)
    }

    /// Download the .csv files for all tables of JOB
    pub fn download_tables(&self, job_kit_config: &JobKitConfig) -> io::Result<()> {
        let done_fpath = self.downloaded_tables_dpath.join("download_tables_done");
        if !done_fpath.exists() {
            log::debug!("[start] downloading tables for {}", job_kit_config);
            // Instructions are from https://cedardb.com/docs/guides/example_datasets/job/, not from the job-kit repo.
            shell::run_command_with_status_check_in_dir(
                &format!("curl -O {JOB_TABLES_URL}"),
                &self.job_dpath,
            )?;
            shell::make_into_empty_dir(&self.downloaded_tables_dpath)?;
            shell::run_command_with_status_check_in_dir(
                "tar -zxvf ../imdb.tgz",
                &self.downloaded_tables_dpath,
            )?;
            shell::run_command_with_status_check_in_dir("rm imdb.tgz", &self.job_dpath)?;
            File::create(done_fpath)?;
            log::debug!("[end] downloading tables for {}", job_kit_config);
        } else {
            log::debug!("[skip] downloading tables for {}", job_kit_config);
        }
        Ok(())
    }

    pub async fn make_parquet_files(
        &self,
        job_kit_config: &JobKitConfig,
        schema_provider: Arc<dyn SchemaProvider>,
    ) -> io::Result<()> {
        let done_fpath = self.downloaded_tables_dpath.join("make_parquet_done");
        if !done_fpath.exists() {
            log::debug!("[start] making parquet for {}", job_kit_config);
            for csv_tbl_fpath in self.get_tbl_fpath_vec("csv").unwrap() {
                let tbl_name = Self::get_tbl_name_from_tbl_fpath(&csv_tbl_fpath);
                let schema = schema_provider
                    .table(&tbl_name)
                    .await
                    .unwrap()
                    .unwrap()
                    .schema();
                let mut parquet_tbl_fpath = csv_tbl_fpath.clone();
                parquet_tbl_fpath.set_extension("parquet");
                let mut opts = Opts::new(csv_tbl_fpath, parquet_tbl_fpath.clone());
                opts.delimiter = ',';
                opts.schema = Some(schema.as_ref().clone());
                csv2parquet::convert(opts).unwrap();
            }
            File::create(done_fpath)?;
            log::debug!("[end] making parquet for {}", job_kit_config);
        } else {
            log::debug!("[skip] making parquet for {}", job_kit_config);
        }
        Ok(())
    }

    /// Convert a tbl_fpath into the table name
    pub fn get_tbl_name_from_tbl_fpath<P: AsRef<Path>>(tbl_fpath: P) -> String {
        tbl_fpath
            .as_ref()
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    /// Get a vector of all generated .csv files in a given directory path
    pub fn get_tbl_fpath_vec(&self, target_ext: &str) -> io::Result<Vec<PathBuf>> {
        let dirent_iter = fs::read_dir(&self.downloaded_tables_dpath)?;

        let entries: Vec<_> = dirent_iter.collect::<Result<Vec<_>, io::Error>>()?;

        let tbl_fpaths: Vec<_> = entries
            .into_iter()
            .filter_map(|dirent| {
                let path = dirent.path();
                if path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext == target_ext)
                    .unwrap_or(false)
                {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();

        Ok(tbl_fpaths)
    }

    /// Get an iterator through all generated .sql files _in order_ of a given config
    /// It's important to iterate _in order_ due to the interface of CardbenchRunnerDBMSHelper
    pub fn get_sql_fpath_ordered_iter(
        &self,
        job_kit_config: &JobKitConfig,
    ) -> io::Result<impl Iterator<Item = (String, PathBuf)>> {
        let queries_dpath = (if job_kit_config.is_light {
            &self.joblight_queries_dpath
        } else {
            &self.job_queries_dpath
        })
        .clone();
        let sql_fpath_ordered_iter =
            job_kit_config
                .query_ids
                .clone()
                .into_iter()
                .map(move |query_id| {
                    let this_genned_query_fpath = queries_dpath.join(format!("{}.sql", &query_id));
                    (query_id, this_genned_query_fpath)
                });
        Ok(sql_fpath_ordered_iter)
    }
}
