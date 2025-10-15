# pipelines-airflow

Airflow DAG collection orchestrating Atlas of Living Australia data ingestion, enrichment, and indexing on Amazon EMR and SOLR.

> Version: **2.0.0** (tested with Apache Airflow 2.10.3 & Python 3.11)

---

## Table of Contents

1. [Overview](#overview)
2. [Compatibility & Versioning](#compatibility--versioning)
3. [Architecture Highlights](#architecture-highlights)
4. [Quick Start](#quick-start)
5. [Configuration](#configuration)
   * [Required MWAA Variables](#required-mwaa-variables)
   * [Airflow Connections](#airflow-connections)
   * [Pools](#pools)
6. [Key DAG Reference](#key-dag-reference)
7. [Ingestion & Partitioning Strategy](#ingestion--partitioning-strategy)
8. [Operations & Monitoring](#operations--monitoring)
9. [Upgrading](#upgrading)
10. [Development](#development)
11. [Troubleshooting](#troubleshooting)
12. [License](#license)

---

## Overview

This repository defines production Airflow DAGs for: bulk dataset ingestion (partitioned by size), provider & single dataset loads, environmental sampling & analytical preprocessing, and SOLR indexing. EMR clusters are launched with category-specific sizing and bootstrap scripts in `dags/sh/` (including CloudWatch Agent setup).

## Compatibility & Versioning

Current version: **2.0.0**

Validated stack:
* Apache Airflow 2.10.3 (see constraints URL in [`dags/requirements.txt`](dags/requirements.txt))
* Python 3.11
* Amazon EMR (cluster sizing & thresholds via config / variables)

If upgrading beyond Airflow 2.10.x re‑validate:
* Dynamic task mapping in `ingest_all_datasets_dag.py`
* Provider compatibility with constraints file
* TriggerDagRun `wait_for_completion` behavior

## Architecture Highlights

* **Dynamic Partitioning** – Datasets sorted by size and allocated into small/large/xlarge categories using per-cluster thresholds.
* **Dynamic Task Mapping** – One mapped `TriggerDagRunOperator` per category (small & large) plus per‑dataset mapping for xlarge.
* **Backpressure via Pools** – Dedicated pools gate parallel EMR cluster (or step) creation.
* **Bootstrap Scripts** – Centralised under `dags/sh/` (cloudwatch agent, pipeline preparation, dataset utilities).
* **Pluggable Index Step** – Full SOLR reindex optionally triggered at end of ingestion run.

## Quick Start

1. Clone repo & deploy DAGs into your Airflow DAG folder.
2. Install Python dependencies referencing Airflow constraint: see `dags/requirements.txt`.
3. Create required Airflow Variables, Connections, and Pools (see below).
4. Unpause desired DAGs (`Ingest_all_datasets` etc.).
5. Trigger `Ingest_all_datasets` with conf (example):

```json
{
  "load_images": "false",
  "skip_dwca_to_verbatim": "false",
  "run_index": "true",
  "override_uuid_percentage_check": "false"
}
```

## Configuration

Central configuration values can live in both Airflow Variables and `ala/ala_config.py`. Keep them consistent.

### Required MWAA Variables

The following Airflow Variables must be configured for the repository to function. They are organized by category and criticality:

#### Core Infrastructure (Essential)

| Variable | Example Value | Purpose |
|----------|---------------|---------|
| `s3_bucket_avro` | `<s3_bucket>` | Primary S3 bucket for Avro datasets and pipeline data |
| `emr_release` | `emr-5.36.2` | EMR release version for main processing clusters |
| `emr_release_preingestion` | `emr-6.15.0` | EMR release for pre-ingestion processing |
| `ec2_subnet_id` | `subnet-xxxxxxxxxxxxxxxxx` | VPC subnet for EMR clusters |
| `job_flow_role` | `<job_flow_role>` | IAM role for EMR EC2 instances |
| `service_role` | `<service_role>` | IAM role for EMR service |
| `ec2_additional_master_security_groups` | `sg-xxxxxxxxxxxxxxxxx` | Security groups for EMR master nodes |
| `ec2_additional_slave_security_groups` | `sg-xxxxxxxxxxxxxxxxx` | Security groups for EMR worker nodes |

#### Cluster Sizing & Performance (Essential)

| Variable | Example Value | Purpose |
|----------|---------------|---------|
| `emr_small_cluster_node_count` | `10` | Number of nodes for small dataset processing |
| `emr_large_cluster_node_count` | `8` | Number of nodes for large dataset processing |
| `emr_xlarge_cluster_node_count` | `3` | Number of nodes for extra-large datasets |
| `emr_small_cluster_total_threshold` | `200000000` | Size threshold for small cluster assignment in Bytes (default: 200MB) |
| `emr_large_cluster_total_threshold` | `5000000000` | Size threshold for large cluster assignment in Bytes (default: 5GB) |
| `ec2_small_instance_type` | `m7g.xlarge` | Instance type for small clusters |
| `ec2_large_instance_type` | `m7g.4xlarge` | Instance type for large clusters |
| `ec2_large_instance_count` | `6` | Instance count of worker nodes for large clusters |
| `ec2_large_ebs_size_in_gb` | `300` | EBS volume size for large instances |
| `min_drs_per_batch` | `40` | Minimum datasets per batch for processing |
| `no_of_dataset_batches` | `6` | Number of parallel dataset batches |

#### ALA Services & APIs (Essential)

| Variable | Example Value | Purpose |
|----------|---------------|---------|
| `ala_api_key` | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` | API key for LA services authentication |
| `ala_api_url` | `https://api.your-domain` | Base URL for LA API services |
| `registry_url` | `https://collections.your-domain/ws/` | Collections registry service URL |
| `biocache_url` | `https://biocache-ws.your-domain/ws` | Biocache web service URL |
| `name_matching_url` | `https://namematching-ws.your-domain/` | Taxonomic name matching service |
| `sampling_url` | `https://sampling.your-domain/sampling-service/` | Environmental sampling service |
| `images_url` | `https://images.your-domain` | Image service URL |
| `lists_url` | `https://lists-ws.your-domain/` | Species lists service URL |

#### SOLR Configuration (Essential)

| Variable | Example Value | Purpose |
|----------|---------------|---------|
| `solr_url` | `http://your-solr-cluster:8983/solr` | SOLR cluster base URL |
| `solr_collection` | `biocache` | Primary SOLR collection name |
| `solr_configset` | `biocache_20240216` | SOLR configuration set to use |
| `solr_collection_rf` | `2` | SOLR replication factor |
| `solr_collection_to_keep` | `biocache` | Collection to retain during cleanups |
| `zk_url` | `zk-host-1,zk-host-2,zk-host-3` | ZooKeeper ensemble for SOLR |

#### Additional Storage & Processing (Important)

| Variable | Example Value | Purpose |
|----------|---------------|---------|
| `s3_bucket_dwca` | `<s3_bucket>` | S3 bucket for Darwin Core Archive files |
| `s3_bucket_dwca_exports` | `<s3_exports_bucket>` | S3 bucket for DwCA exports |
| `s3_bucket_ala_uploaded` | `<s3_uploaded_bucket>` | S3 bucket for user-uploaded datasets |
| `preingestion_ami` | `ami-xxxxxxxxxxxxxxxxx` | AMI for pre-ingestion processing |
| `preingestion_ebs_size_in_gb` | `50` | EBS size for pre-ingestion instances |
| `dr_rec_count_threshold` | `100000` | Record count threshold for processing decisions |
| `master_market` | `ON_DEMAND` | EC2 market type for master nodes |
| `slave_market` | `ON_DEMAND` | EC2 market type for worker nodes |

#### Optional Features

| Variable | Example Value | Purpose |
|----------|---------------|---------|
| `environment` | `TEST` | Environment identifier (TEST/PROD) |
| `slack_notification` | `False` | Enable Slack notifications |
| `slack_alerts_channel` | `#airflow-alerts-public` | Slack channel for alerts |
| `alert_email` | `None` | Email for alerts (if not using Slack) |
| `dashboard_cache_clear_url` | `https://dashboard.your-domain/clearCache` | Dashboard cache clearing endpoint |
| `load_images` | `false` | Default setting for image loading |

#### Authentication (Optional - for secured environments)

| Variable | Example Value | Purpose |
|----------|---------------|---------|
| `auth_client_id` | `xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx` | OAuth client ID |
| `auth_client_secret` | `xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx` | OAuth client secret |
| `auth_scope` | `<auth_scope>` | OAuth scope for API access |
| `ala_oidc_url` | `https://your-oauth-provider.com/oauth2/token` | OAuth token endpoint |

### Airflow Connections

| Connection Id | Type | Purpose |
|---------------|------|---------|
| `aws_default` | Amazon Web Services | S3 + EMR APIs |
| `slack_default` | HTTP/Slack | Notifications (optional) |

### Pools

| Pool | Purpose | Sizing Guideline |
|------|---------|------------------|
| `ingest_all_small_pool` | Small dataset batch triggers | = small clusters allowed |
| `ingest_all_large_pool` | Large dataset batch triggers | = large clusters allowed |
| `ingest_all_xlarge_pool` | Per‑dataset xlarge triggers | Cap heavy workloads |

CLI creation:

```bash
airflow pools set ingest_all_small_pool 4 "Small ingest batches"
airflow pools set ingest_all_large_pool 2 "Large ingest batches"
airflow pools set ingest_all_xlarge_pool 3 "XLarge per-dataset ingest"
```

## Key DAG Reference

| DAG | Purpose |
|-----|---------|
| `load_dataset_dag.py` | Ingest a single dataset end-to-end (excl. SOLR index) |
| `load_provider_dag.py` | Ingest all datasets for a data provider | 
| `ingest_small_datasets_dag.py` | Batch many small datasets on single-node clusters |
| `ingest_large_datasets_dag.py` | Batch many large datasets on multi-node clusters | 
| `ingest_all_datasets_dag.py` | Master orchestrator (partition + mapped triggers + optional index) |
| `full_index_to_solr.py` | Full SOLR indexing | 
| `solr_dataset_indexing.py` | Index a single dataset into live SOLR |

## Ingestion & Partitioning Strategy

1. List all candidate datasets (DwCA / Avro) in S3.
2. Sort ascending by size; distribute through small & large categories via per‑cluster cumulative threshold (round‑robin layered assignment).
3. Remaining datasets become xlarge (unbounded) and each is triggered individually.
4. Pools regulate concurrency; each mapped trigger optionally waits for completion.
5. Optional full index DAG executes after all ingestion pathways finish.

Benefits: balanced cluster utilization, predictable parallelism, fine-grained control of heavy datasets, reduced wasted capacity.

## Operations & Monitoring

| Aspect | Guidance |
|--------|----------|
| Pools saturation | UI: Admin → Pools; queued tasks show slot shortages |
| EMR cost control | Tune pool sizes & thresholds; disable unneeded categories |
| Logs | Task logs + EMR step logs (CloudWatch if agent enabled) |
| Notifications | Slack webhook (success/failure operator) |
| Fail fast | Partition task raises skip if zero datasets |

## Upgrading

1. Update Airflow version & constraints URL (match minor release) in `dags/requirements.txt`.
2. Recreate pools in new environments (names must match constants in DAG code).
3. Validate provider versions (Slack, AWS) after constraint bump.
4. Re-test dynamic mapping UI for xlarge dataset fan-out.
5. Confirm EMR bootstrap scripts remain compatible with new EMR release family.

## Development

| Topic | Command / Notes |
|-------|-----------------|
| Formatting | `black .` (line length 120) |
| Python version | 3.11 (see `pyproject.toml`) |
| Dependency mgmt | Runtime pinned by Airflow constraints; local dev tools under dev extras |
| Adding a DAG | Place in `dags/`; follow existing logging & TaskGroup patterns |

## Troubleshooting

| Symptom | Possible Cause | Action |
|---------|----------------|--------|
| Tasks stuck queued | Pool slots exhausted | Adjust pool size or reduce parallel triggers |
| KeyError on Variable | Missing Airflow Variable | Create variable with expected name |
| EMR cluster fails bootstrap | Out-of-date script | Review `dags/sh/` logs & update script |
| No xlarge tasks appear | Partition produced none | Expected if all datasets fit earlier categories |
| Index skipped | `run_index` false in conf | Re-trigger with `run_index=true` |

## License

Distributed under the Mozilla Public License 1.1. See [`LICENSE.md`](LICENSE.md).

---

For questions or improvements, open an issue or submit a PR—contributions welcome.





