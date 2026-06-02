# Youtube_GeekCoders_Project

<img width="2408" height="1371" alt="image" src="https://github.com/user-attachments/assets/9cf6f8b9-ea3d-4e8e-801c-2b42cb92b428" />


This repository contains my learning project following a YouTube tutorial on building and deploying a Databricks data engineering project using **Databricks Asset Bundles**, **Delta Live Tables / Lakeflow Declarative Pipelines**, **GitHub**, and **GitHub Actions CI/CD**.

The project focuses on ingesting earthquake data, storing raw files in a Databricks Volume, transforming the data through a medallion-style architecture, and deploying Databricks resources using Databricks Asset Bundles.

---

## Project Objective

The main objective of this project is to learn how to build a modern Databricks data pipeline with proper development and production deployment practices.

This project covers:

- Databricks Asset Bundles
- GitHub version control
- GitHub Actions CI/CD
- Dev and prod deployment targets
- Databricks Volumes
- Delta tables
- Delta Live Tables / Lakeflow Declarative Pipelines
- Automated bundle deployment
- Basic production deployment structure

---

## Project Architecture

```text
USGS Earthquake API
        ↓
Raw JSON files
        ↓
Databricks Volume - Bronze Layer
        ↓
Auto Loader / Streaming Read
        ↓
DLT View / Transformation Logic
        ↓
Silver Delta Table
        ↓
Dashboard / Job / Pipeline Resources
```

---

## Repository Structure

```text
Youtube_GeekCoders_Project/
│
├── .github/
│   └── workflows/
│       └── deploy-databricks.yml
│
└── youtube_project_bundle/
    │
    ├── databricks.yml
    ├── resources/
    │   ├── jobs.yml
    │   ├── pipelines.yml
    │   └── dashboards.yml
    │
    └── src/
        └── notebooks / pipeline files
```

---

## Technologies Used

- Databricks
- Databricks Asset Bundles
- Databricks CLI
- GitHub
- GitHub Actions
- Python
- PySpark
- Delta Lake
- Unity Catalog
- Databricks Volumes
- Delta Live Tables / Lakeflow Declarative Pipelines

---

## Databricks Asset Bundle

The main bundle configuration is defined in:

```text
youtube_project_bundle/databricks.yml
```

The bundle contains two deployment targets:

```yaml
targets:
  dev:
    mode: development

  prod:
    mode: production
```

The `dev` target is used for development testing, while the `prod` target is intended for production deployment through GitHub Actions.

---

## Development Workflow

The development workflow follows this process:

```text
develop branch
      ↓
make changes
      ↓
commit and push
      ↓
create pull request
      ↓
merge into main
      ↓
GitHub Actions deploys to Databricks prod
```

---

## Git Commands Used

Clone the repository:

```bash
git clone https://github.com/umarulaiman86/Youtube_GeekCoders_Project.git
```

Switch to the `develop` branch:

```bash
git checkout develop
```

Check file changes:

```bash
git status
```

Commit changes:

```bash
git add .
git commit -m "update databricks bundle"
```

Push to GitHub:

```bash
git push origin develop
```

---

## GitHub Actions CI/CD

The GitHub Actions workflow is triggered when changes are merged into the `main` branch.

```yaml
on:
  push:
    branches:
      - main
```

The workflow performs these steps:

1. Checkout repository code
2. Setup Databricks CLI
3. Check Databricks identity
4. Validate Databricks Asset Bundle
5. Deploy Databricks Asset Bundle to the `prod` target

Example workflow file:

```yaml
name: Deploy Databricks Asset Bundle

on:
  push:
    branches:
      - main

jobs:
  deploy:
    runs-on: ubuntu-latest

    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
      DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main

      - name: Check Databricks current user
        run: databricks current-user me

      - name: Validate Databricks Bundle
        working-directory: ./youtube_project_bundle
        run: databricks bundle validate -t prod

      - name: Deploy Databricks Bundle
        working-directory: ./youtube_project_bundle
        run: databricks bundle deploy -t prod
```

---

## GitHub Secrets Required

The following GitHub repository secrets are required for proper production deployment using a Databricks service principal:

```text
DATABRICKS_HOST
DATABRICKS_CLIENT_ID
DATABRICKS_CLIENT_SECRET
```

Example:

```text
DATABRICKS_HOST = https://dbc-0b58cee6-85ce.cloud.databricks.com
```

For early testing with a personal access token, this can also be used:

```text
DATABRICKS_TOKEN
```

However, for proper production deployment, a Databricks service principal is recommended instead of a personal user token.

---

## Databricks Bundle Deployment

Validate the bundle locally for development:

```bash
databricks bundle validate -t dev
```

Deploy to development:

```bash
databricks bundle deploy -t dev
```

Validate production target:

```bash
databricks bundle validate -t prod
```

Deploy to production:

```bash
databricks bundle deploy -t prod
```

---

## Data Source

This project uses earthquake data from the USGS earthquake feed.

The raw API response is saved as JSON into a Databricks Volume before being processed into structured Delta tables.

Example raw storage path:

```text
/Volumes/<catalog>/bronze/earthquake_data/
```

---

## Medallion Architecture

This project follows a simple medallion architecture.

### Bronze Layer

Stores raw earthquake JSON files.

```text
Raw API response → Databricks Volume
```

### Silver Layer

Cleans and transforms the raw earthquake data into a structured Delta table.

```text
Bronze JSON → Parsed and cleaned earthquake table
```

### Gold Layer

Can be extended later for business-level aggregations, dashboards, and reporting.

```text
Silver Delta Table → Aggregated tables / dashboards / reports
```

---

## Example Earthquake Pipeline Flow

The earthquake pipeline follows this basic flow:

```text
1. Call USGS earthquake API
2. Save raw JSON response into Databricks Volume
3. Read new JSON files using Auto Loader
4. Parse nested JSON fields
5. Extract earthquake properties and coordinates
6. Convert timestamp and numeric fields
7. Apply changes into final Silver Delta table
```

Example transformation concept:

```python
@dlt.view(name="earthquake_data_vw")
def earthquake_data():
    df = (
        spark.readStream
        .format("cloudfiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
    )

    return transformed_df
```

Final table concept:

```python
dlt.create_streaming_table(name="earthquake_data_final")

dlt.apply_changes(
    target="earthquake_data_final",
    source="earthquake_data_vw",
    keys=["id"],
    sequence_by=col("_load_timestamp"),
    stored_as_scd_type=1
)
```

---

## Dev and Prod Targets

The `dev` target is used for testing and development.

Example:

```yaml
dev:
  mode: development
  default: true
  workspace:
    host: https://dbc-0b58cee6-85ce.cloud.databricks.com
  variables:
    catalog: youtube_dev
    schema: dev
```

The `prod` target is used for production deployment.

Example:

```yaml
prod:
  mode: production
  workspace:
    host: https://dbc-0b58cee6-85ce.cloud.databricks.com
    root_path: /Workspace/Shared/youtube_project_bundle_prod
  variables:
    catalog: youtube_prod
    schema: prod
```

---

## Proper Production Deployment Setup

For production, the recommended setup is to use a Databricks service principal instead of a personal access token.

Recommended architecture:

```text
GitHub Actions
        ↓
Databricks Service Principal
        ↓
Databricks Asset Bundle deploy
        ↓
Prod Databricks jobs / pipelines / dashboards
```

The service principal should have proper permissions to manage:

- Workspace deployment folder
- Jobs
- Pipelines
- SQL warehouse
- Unity Catalog catalog and schema
- Bundle resources

Example bundle permission:

```yaml
permissions:
  - service_principal_name: <service-principal-id>
    level: CAN_MANAGE
  - user_name: aimanramli.8@gmail.com
    level: CAN_MANAGE
```

---

## Common Issues Faced

### 1. GitHub branch not updated

Databricks deployment does not automatically update GitHub. Changes must be committed and pushed using Git.

```bash
git add .
git commit -m "update bundle config"
git push origin develop
```

### 2. Wrong folder in PowerShell

If Git shows this error:

```text
fatal: not a git repository
```

It means PowerShell is not inside the GitHub repo folder.

Fix:

```powershell
cd "C:\Users\umarul.ramli\Documents\Youtube_GeekCoders_Project"
git status
```

### 3. Missing Databricks variable

If a variable is declared in `databricks.yml`, it must be assigned under the target.

Example:

```yaml
variables:
  spn_id:
    description: spn id
```

Target assignment:

```yaml
variables:
  spn_id: <service-principal-id>
```

### 4. Databricks token permission issue

A personal access token may not have enough permission scopes for production deployment.

Example error:

```text
Provided access token does not have required scopes: access-management
```

For production, use a Databricks service principal with proper workspace and bundle permissions.

### 5. Bundle deployment lock issue

Example error:

```text
Failed to acquire deployment lock
access denied: state/deploy.lock
```

This usually means the deployment identity does not have enough permission to manage the bundle deployment folder or apply bundle permissions.

---

## Key Learning Outcomes

Through this project, I learned how to:

- Structure a Databricks project using Asset Bundles
- Use GitHub branches for development and production workflow
- Deploy Databricks resources using GitHub Actions
- Separate dev and prod configurations
- Use Databricks Volumes for raw file storage
- Build a basic streaming pipeline using Auto Loader and DLT
- Manage Databricks bundle variables and deployment targets
- Troubleshoot GitHub Actions and Databricks permission issues
- Understand why service principals are preferred for production CI/CD

---

## Future Improvements

Planned improvements for this project:

- Complete proper Databricks service principal production setup
- Add full production deployment permission setup
- Add automated tests for notebooks and pipelines
- Add Gold layer aggregation tables
- Add dashboard screenshots
- Improve documentation for each pipeline step
- Add data quality expectations in DLT / Lakeflow pipeline
- Add monitoring and alerting for failed pipeline runs

---

## Project Status

This project is currently used as a learning project to understand Databricks CI/CD and Asset Bundle deployment.

Current focus:

```text
Databricks Asset Bundle
GitHub Actions
Dev to Prod deployment workflow
Earthquake data pipeline
Production deployment with service principal
```

---

## Acknowledgement

This project is based on a YouTube tutorial by GeekCoders and is used for learning purposes. Additional modifications were made while practicing GitHub Actions, Databricks Asset Bundles, CI/CD, and production deployment setup.

