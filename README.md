📦 Intelligent Inventory Management — DataOps Sandbox

Modern DataOps + DataSecOps for inventory analytics using dbt, Snowflake, and GitHub Actions.
This lab demonstrates how DataSecOps development pipelines are managed.
When a new feature request is opened in GitHub, the pipeline automatically provisions a dedicated Snowflake environment using the same dbt models that power production.
This environment is fully isolated, named for the issue, and seeded with the necessary tables so you can iterate safely.

Highlights: GitFlow‑aligned CI/CD, opt‑in orchestration via commit “run‑tags”, environment‑aware deployments, security scanning, and metrics publishing.

Table of Contents

Features

Architecture

Repository Layout

Requirements

Setup

How Orchestration Triggers Work

Quick Start

Local Development

Observability

Security

Troubleshooting

Contributing

License

Support

Features

Tiered ELT (Medallion): dbt models for bronze → silver → gold with seeds and tests.

Environment Resolution: main → prod, develop → dev, feature/* → ci_cd.

Issue‑Scoped Environments: Each GitHub issue labeled feature provisions a fully isolated schema on Snowflake.
The orchestrator uses dbt to build the models in this schema so you can develop or experiment without impacting shared environments.

GitHub Actions Orchestrator: Security → ELT → Observability with strict dependency gates.

Opt‑in Push Runs: Pipelines only run when commit messages include specific run‑tags.

Issue‑Driven Provisioning: Label an issue feature to provision a temporary schema (merged into the above bullet for clarity).

Observability: Automated metrics job, dashboards, and Slack summaries.

Security: SAST/linting and policy docs baked in.

Architecture
GitHub → Orchestrator (Actions)
     ├─ 🔒 Security (SAST/linting)
     ├─ 🛠️ ELT (dbt on Snowflake)
     └─ 📊 Observability (metrics & reporting)


Snowflake hosts schemas, tables, and compute.

dbt builds and tests models; macros enable dynamic naming by branch/issue.

GitHub Actions coordinates stages and posts Slack notifications.

Python utilities handle provisioning/cleanup, dynamic environment creation, and dashboard generation.

Repository Layout
.
├── docs/
│   ├── 00_services_configuration.md
│   ├── 01_dbt_seed_data.md
│   ├── 02_dbt_dynamic_macros.md
│   ├── 03_dbt_models.md
│   ├── 04_snowflake_setup.md
│   ├── 05_github_actions_automation.md
│   ├── 06_github_issue_templates.md
│   └── 07_security_policy.md
├── scripts/
│   ├── dbt/
│   │   ├── macros/dynamic_naming.sql
│   │   ├── models/
│   │   │   ├── bronze/
│   │   │   ├── silver/
│   │   │   └── gold/
│   │   ├── dbt_project.yml
│   │   ├── packages.yml
│   │   ├── profiles.yml
│   │   └── seeds/
│   ├── ddls/
│   │   ├── create_schema.sql
│   │   ├── create_tables.sql
│   │   ├── dashboard_metrics.sql
│   │   └── grant_permissions.sql
│   └── python/
│       ├── create_dashboard.py
│       ├── create_schema.py
│       ├── drop_schema.py
│       ├── create_seed.py
│       ├── run_metrics.py
│       └── requirements.txt
├── .github/
│   ├── ISSUE_TEMPLATE/
│   │   ├── feature_request.yml
│   │   └── cleanup_request.yml
│   └── workflows/
│       ├── orchestrator.yml         # Security → ELT → Observability (gated)
│       ├── data_pipeline.yml        # Called by orchestrator
│       ├── security.yml             # Called by orchestrator
│       └── observability.yml        # Called by orchestrator
├── README.md
└── LICENSE


Each doc in docs/ maps to a hands‑on lab or implementation guide for this sandbox.

Requirements

Snowflake account: create one at https://signup.snowflake.com

GitHub repository: with Actions enabled

Slack Incoming Webhook: for orchestration summaries (optional but recommended)

Local tooling (optional): python 3.10+, dbt-core + dbt-snowflake, jq

Setup

Clone & open the repo.

Configure GitHub secrets (Repository → Settings → Secrets and variables → Actions):

SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_TOKEN,
SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE

SLACK_WEBHOOK_URL (optional)

(Optional) Set repository variables (to change stage defaults):

RUN_SECURITY=yes|no, RUN_OBSERVABILITY=yes|no

Review docs:

Services & accounts: docs/00_services_configuration.md

Snowflake setup: docs/04_snowflake_setup.md

dbt structure: docs/03_dbt_models.md

Orchestration: docs/05_github_actions_automation.md

How Orchestration Triggers Work

The main coordinator is .github/workflows/orchestrator.yml. It will only proceed when one of the following is true:

Manual run: Run workflow (workflow_dispatch).

Issue labeled feature or cleanup: provisions or cleans up resources.

When an issue is labeled feature, the orchestrator creates a new environment on Snowflake.
Using scripts/python/create_schema.py, it dynamically names a schema based on the issue ID, seeds it with data, and invokes dbt models to build that schema.
This environment remains isolated until the issue is closed and cleanup is triggered.

Push with run‑tags in the commit message (opt‑in):

#run_all – run everything

#orchestrate – run orchestration

Stage‑specific:

#run_security

#run_elt, #run_pipeline, or #run_pipelines

#run_obs or #run_observability

Skip tags:

#skip_all, #skip_orchestrate, #skip_security, #skip_elt, #skip_pipeline(s), #skip_obs, #skip_observability

No run‑tags in a push ⇒ pipeline is quiet. No stages and no Slack.

Quick Start
A) Validate the whole pipeline via tag (demo path)
git commit -am "Pipeline validation"
git tag "pipeline validation"
git push origin --tags

B) Opt‑in run from a commit message
git commit -am "Add daily snapshot #orchestrate #run_elt"
git push

C) Provision a feature environment from an issue

Open a new GitHub Issue using the Feature Request template.

Ensure it carries the feature label and includes the object name.

The orchestrator will create a dedicated Snowflake schema for that issue during the next run, build dbt models in it, and post a Slack notification with the environment name.

Local Development

Install Python deps (optional but useful for utilities):

python -m venv .venv && source .venv/bin/activate
pip install -r scripts/python/requirements.txt


Install dbt deps:

cd scripts/dbt
dbt deps


Seed & build (against your Snowflake target in profiles.yml):

dbt seed
dbt run
dbt test


Macros & dynamic naming: see scripts/dbt/macros/dynamic_naming.sql and docs/02_dbt_dynamic_macros.md.

Observability

Metrics SQL lives in scripts/ddls/dashboard_metrics.sql and scripts/python/run_metrics.py.
The latter pulls INFORMATION_SCHEMA metrics to surface query counts, failures, durations, and storage across all environments.

The Observability workflow publishes metrics and can update dashboards.

Slack summary highlights stage outcomes and links to the run.

Security

Security policies and boundaries are documented in docs/07_security_policy.md.

The Security workflow (SAST/linting) runs as the first stage when triggered.

Use skip/run tags to control scope per commit.

Troubleshooting

Push didn’t run: confirm your commit message includes a run‑tag (e.g., #orchestrate, #run_elt). Amended commits must be pushed for Actions to reevaluate.

Jobs skipped but Slack fired: ensure you’re on the latest orchestrator.yml where notify only runs when gate.proceed == 'true'.

Snowflake auth errors: verify secrets and role/warehouse/database values. Test with:

snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER


dbt profile not found: confirm profiles.yml location and active target.

Contributing

Fork and create a branch from develop.

Use clear commit messages and optional run‑tags to control CI.

Open a PR; the orchestrator and stage workflows will validate changes.

License

This project is released under the MIT License
.

Support

Questions or issues? Reach out:

Open a GitHub Issue using the provided templates.
