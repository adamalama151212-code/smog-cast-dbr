# Final Project Specification

## Goal

Each student delivers their own complete, production-style data platform, built across the 12 weeks on their chosen dataset — from ingestion all the way to an AI application, deployed to PROD entirely through CI/CD.

## Scope

This is an individual project — the through-line of the whole course. The two team demos (after Weeks 3 and 7) were collaboration checkpoints; the Final Project is your own portfolio piece.

## Must Include

* Two environments: develop in the DEV workspace, deploy to paid PROD — nothing built by hand in PROD
* A full medallion (bronze → silver → gold) with batch and streaming ingestion, governed by Unity Catalog (catalogs / schemas / volumes, RLS/CLS), secrets in Key Vault
* Schema evolution and data quality handled (expectations / contracts); idempotent, re-runnable pipelines
* At least one declarative (Lakeflow) pipeline and one Lakeflow Job
* A test suite (unit tests + data-quality checks) wired into CI; deployment via Databricks Asset Bundles + CI/CD (GitHub Actions / Azure DevOps)
* A gold-layer analytics dashboard answering real business questions
* One AI application (RAG / Knowledge Assistant) on top of the platform
* At least one advanced capability of choice: Lakehouse Federation, CDC, REST API automation, or Zerobus

## Deliverables

* A Git repository with a README and an architecture diagram
* A working Asset Bundle that deploys to PROD; a green CI pipeline; passing tests
* The running platform: pipelines, jobs, dashboard, and AI application
* A short write-up: design decisions, cost / performance trade-offs, and where AI-assisted development helped (and the guardrails used)

## Demo Day

A 20–30 minute presentation: the architecture, a live DEV → PROD deployment through CI/CD, a pipeline run with data quality, the dashboard, and the AI application — followed by Q&A.

## Evaluation Rubric

* **End-to-end correctness:** raw → gold flows idempotently and is re-runnable
* **Governance & security:** Unity Catalog + RLS/CLS, with secrets in Key Vault
* **Engineering quality:** tests pass, code is modular, and the Git history is clean
* **Automation:** the project deploys to PROD with no manual steps
* **AI & analytics:** a working dashboard and a useful AI application
* **Communication:** a clear architecture story and an honest discussion of trade-offs
