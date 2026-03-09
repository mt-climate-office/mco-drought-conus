# MCO CONUS Drought Pipeline

A gridded drought monitoring pipeline for the contiguous United States (CONUS) built on
[GridMET](https://www.climatologylab.org/gridmet.html) climate data. Produces Cloud-Optimized
GeoTIFF (COG) rasters for multiple drought and climate stress indicators across a range of
timescales, updated operationally via Docker.

---

## Metrics Produced

| Indicator | Description |
|-----------|-------------|
| **SPI** | Standardized Precipitation Index |
| **% of Normal** | Precipitation as percent of climatological normal |
| **Deviation** | Precipitation departure from normal (mm) |
| **Percentile** | Precipitation percentile rank |
| **SPEI** | Standardized Precipitation-Evapotranspiration Index |
| **EDDI** | Evaporative Demand Drought Index |
| **SVPDI** | Standardized VPD Index (vapor pressure deficit) |
| **Tmax Percentile** | Maximum temperature percentile rank |

**Timescales:** 15d, 30d, 45d, 60d, 90d, 120d, 180d, 365d, 730d, water year, year-to-date (YTD)

---

## Pipeline

Scripts run sequentially inside the container via `run_once.sh`:

```
1_gridmet-cache.R          Download / refresh raw GridMET NetCDF files
        |
        v
2_precipitation-metrics.R  SPI, % of normal, deviation, percentile
        |
        v
3_metrics-spei.R           SPEI
        |
        v
4_metrics-eddi.R           EDDI
        |
        v
5_metrics-vpd.R            SVPDI (VPD-based)
        |
        v
6_metrics-tmax.R           Tmax percentile
```

All outputs land in `$DATA_DIR/derived/conus_drought/` as COG GeoTIFFs.

---

## Repository Structure

```
mco-drought-conus/
├── R/
│   ├── 1_gridmet-cache.R
│   ├── 2_precipitation-metrics.R
│   ├── 3_metrics-spei.R
│   ├── 4_metrics-eddi.R
│   ├── 5_metrics-vpd.R
│   ├── 6_metrics-tmax.R
│   └── drought-functions.R
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── run_once.sh
├── scripts/
│   └── ecr-push.sh            # Build and push Docker image to ECR
├── terraform/                 # AWS infrastructure (see Cloud Architecture below)
│   ├── main.tf
│   ├── variables.tf
│   ├── locals.tf
│   ├── outputs.tf
│   ├── s3.tf
│   ├── ecr.tf
│   ├── iam.tf
│   ├── vpc.tf
│   ├── ecs.tf
│   ├── scheduler.tf
│   ├── cloudwatch.tf
│   └── terraform.tfvars.example
├── .gitignore
└── README.md
```

Expected data layout (outside the repo, mounted at runtime):

```
$DATA_DIR/                         # e.g. ~/mco-drought-conus-data
├── raw/                           # GridMET .nc files (pr, pet, vpd, tmmx)
├── interim/                       # intermediate tiles (removed if KEEP_TILES=0)
├── derived/
│   └── conus_drought/             # final COG GeoTIFFs
└── tmp/                           # scratch space for terra / R
```

---

## Quick Start (Local Docker)

**Prerequisites:** Docker Desktop (or Docker Engine + Compose plugin).

```bash
# 1. Clone the repo
git clone https://github.com/mt-climate-office/mco-drought-conus.git
cd mco-drought-conus

# 2. Build the image (only needed once, or after Dockerfile/R script changes)
docker compose -f docker/docker-compose.yml build

# 3. Run — same command every time, including first run
docker compose -f docker/docker-compose.yml up
```

All processed data is written to `~/mco-drought-conus-data` on your host (outside the repo,
never tracked by git). Override the data directory with the `DATA_DIR` environment variable:

```bash
DATA_DIR=/Volumes/my-drive/drought-data docker compose -f docker/docker-compose.yml up
```

S3 sync is **automatically skipped** in local runs — `AWS_BUCKET` is not set by
docker-compose, so no credentials or AWS access are required.

> **Note:** The build context is the repo root (not `docker/`), so Docker can copy the `R/`
> scripts into the image. The source volume mount uses a relative path (`..`) so the compose
> file works regardless of where the repo is cloned.

The container mounts two host directories:

| Host path | Container path |
|-----------|----------------|
| `~/mco-drought-conus` | `/home/rstudio/mco-drought-conus` |
| `~/mco-drought-conus-data` | `/home/rstudio/mco-drought-conus-data` |

Adjust the `volumes` block in `docker-compose.yml` if your data directory lives elsewhere.

### Cold-Start / First-Run Notes

**The data directory is created automatically** — if `~/mco-drought-conus-data` does not exist,
`run_once.sh` creates it (and all subdirectories) before downloading anything.

**`docker compose up` is the same command every time**, including the very first run. The
GridMET download step runs in two phases on every invocation:

1. **Historical fill** — downloads any year files missing from disk (skips existing files).
   On a cold start this pulls the full record from `START_YEAR` (default 1991) to present:
   4 variables × ~35 annual NetCDF files (~140 files, each 50–200 MB). Expect several hours
   on a first run; on subsequent runs this phase completes almost instantly because the files
   already exist.

2. **Recent refresh** — always deletes and re-downloads the last `GRIDMET_REFRESH_YEARS`
   years (default: 2) for every variable, regardless of whether those files are already
   present. This ensures preliminary/updated GridMET data is replaced on every run.

Because the two phases are separate, a partial cache (e.g. `pr` and `pet` already downloaded
but `vpd` and `tmmx` not yet) is handled correctly — phase 1 fills in only what is missing,
and phase 2 refreshes the recent years for all variables.

---

## Environment Variables

Override any of these in `docker-compose.yml` under `environment:`, or pass them with
`docker compose run -e VAR=value`.

| Variable | Default | Description |
|----------|---------|-------------|
| `CORES` | `8` | Parallel workers for tile processing |
| `KEEP_TILES` | `0` | Set to `1` to retain intermediate tile folders after the run |
| `CONUS_MASK` | `1` | Apply CONUS land mask to outputs (`0` = no mask) |
| `TILE_DX` | `2` | Tile width in degrees |
| `TILE_DY` | `2` | Tile height in degrees |
| `GRIDMET_REFRESH_YEARS` | `2` | Number of most-recent years to force-delete and re-download on every run |
| `START_YEAR` | `1991` | Earliest year to include in the historical fill (phase 1) |
| `DATA_DIR` | `~/mco-drought-conus-data` | Root directory for raw, interim, and derived data |
| `CLIM_PERIODS` | `rolling:30` | Comma-separated climatological reference period specs (see below) |

### `CLIM_PERIODS` syntax

Each spec produces a slug appended to all output filenames:

| Spec | Description | Output slug |
|------|-------------|-------------|
| `rolling:N` | Last N years from current date | `rolling_N` |
| `fixed:YYYY:YYYY` | Fixed year range (inclusive) | `fixed_YYYY_YYYY` |
| `full` | All years from `START_YEAR` to present | `full` |

Multiple specs are comma-separated. Each produces its own set of output files:

```yaml
# Single period (default — matches prior behavior)
CLIM_PERIODS: "rolling:30"

# Two periods in one run
CLIM_PERIODS: "rolling:30,fixed:1991:2020"

# Fixed baseline only
CLIM_PERIODS: "fixed:1991:2020"

# Full record
CLIM_PERIODS: "full"
```

Output files in `$DATA_DIR/derived/conus_drought/` are slug-tagged, e.g.:
```
spi_30d_rolling_30.tif
spi_30d_fixed_1991_2020.tif
spei_15d_rolling_30.tif
```

---

## Cloud Architecture (AWS)

The pipeline runs nightly on AWS Fargate, triggered by an EventBridge Scheduler rule at
**6:00 PM Mountain Time** (DST-aware). Outputs are written to a public S3 bucket.
All infrastructure is managed with Terraform in the `terraform/` directory.

### Architecture Overview

```
EventBridge Scheduler (6 PM Mountain)
        │
        ▼
  ECS Fargate Task  ──────────────────────────────────────────┐
  (8 vCPU / 32 GB / 200 GiB ephemeral)                       │
        │                                                      │
        ▼                                                      ▼
  S3: mco-gridmet/cache/          (restore at start / save at end)
  S3: mco-gridmet/derived/        (final COG GeoTIFF outputs, public)
        │
  ECR: mco-drought-conus          (Docker image)
  CloudWatch: /ecs/mco-drought-conus  (logs, 90-day retention)
```

### AWS Resources

| Resource | Name / ID | Purpose |
|----------|-----------|---------|
| S3 bucket | `mco-gridmet` | Public outputs + GridMET cache |
| ECR repository | `mco-drought-conus` | Docker image registry |
| ECS cluster | `mco-drought-conus` | Fargate compute |
| ECS task definition | `mco-drought-conus` | 8 vCPU, 32 GB RAM, 200 GiB ephemeral |
| EventBridge Scheduler | `mco-drought-conus-nightly` | `cron(0 18 * * ? *)` / `America/Denver` |
| CloudWatch log group | `/ecs/mco-drought-conus` | Pipeline logs (90-day retention) |
| IAM roles | `mco-drought-conus-task`, `-task-execution`, `-scheduler` | Least-privilege permissions |

### S3 Bucket Layout

```
s3://mco-gridmet/
├── cache/
│   ├── raw/          # GridMET annual NetCDF files (pr, pet, vpd, tmmx)
│   └── interim/      # Processed intermediate tiles
└── derived/
    └── conus_drought/  # Final COG GeoTIFFs (public read)
```

### Caching Strategy

Because Fargate ephemeral storage is wiped when a task stops, GridMET data is cached in S3:

| Run | Behavior |
|-----|----------|
| Cold start (first ever) | Downloads ~15–20 GB from GridMET servers; saves to `s3://mco-gridmet/cache/` |
| Every subsequent run | Restores cache from S3 (~2–3 min); refreshes last 2 years from GridMET; saves updates back |

---

### Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) ≥ 1.5
- [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- AWS SSO configured in `~/.aws/config` with a profile named `mco`

### First-Time Deployment

```bash
# 1. Authenticate
aws sso login --profile mco

# 2. Create your tfvars (copy the example and fill in your values)
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform/terraform.tfvars with your account ID, VPC ID, subnet IDs, etc.

# 3. Deploy infrastructure
cd terraform
terraform init
terraform apply

# 4. Build and push the Docker image
cd ..
./scripts/ecr-push.sh
```

After `terraform apply` completes, the outputs show all resource identifiers:

```
ecr_repository_url      = 123456789012.dkr.ecr.us-west-2.amazonaws.com/mco-drought-conus
ecs_cluster_name        = mco-drought-conus
ecs_task_definition_arn = arn:aws:ecs:us-west-2:...:task-definition/mco-drought-conus:1
s3_bucket_url           = https://mco-gridmet.s3.us-west-2.amazonaws.com
scheduler_arn           = arn:aws:scheduler:us-west-2:...:schedule/default/mco-drought-conus-nightly
cloudwatch_log_group    = /ecs/mco-drought-conus
```

### Triggering a Manual Run

```bash
aws ecs run-task \
  --cluster mco-drought-conus \
  --task-definition mco-drought-conus \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={
    subnets=[subnet-xxxxxxxxxxxxxxxxx],
    securityGroups=[sg-xxxxxxxxxxxxxxxxx],
    assignPublicIp=ENABLED
  }" \
  --profile mco --region us-west-2
```

### Monitoring

```bash
# Watch logs from a running task
aws logs tail /ecs/mco-drought-conus --follow --profile mco --region us-west-2

# Check task status
aws ecs describe-tasks \
  --cluster mco-drought-conus \
  --tasks <task-id> \
  --profile mco --region us-west-2 \
  --query 'tasks[0].{status:lastStatus,stopReason:stoppedReason}'
```

### Redeploying After Code Changes

```bash
aws sso login --profile mco   # if SSO session has expired
./scripts/ecr-push.sh         # rebuilds image and pushes :latest to ECR
```

The scheduler always pulls `:latest`, so the next nightly run automatically uses the new image.
No Terraform changes are needed for code-only updates.

### `terraform.tfvars` Reference

`terraform/terraform.tfvars` is gitignored. Copy `terraform.tfvars.example` and populate:

| Variable | Description |
|----------|-------------|
| `aws_region` | AWS region (e.g. `us-west-2`) |
| `aws_profile` | SSO profile name in `~/.aws/config` |
| `aws_account_id` | 12-digit AWS account ID |
| `vpc_id` | Existing VPC ID |
| `subnet_ids` | List of subnet IDs for Fargate tasks (must have internet access) |
| `extra_security_group_ids` | Existing security group(s) to attach to Fargate tasks |
| `s3_bucket_name` | S3 bucket for outputs (default: `mco-gridmet`) |

---

## Data Source

Raw climate data comes from **GridMET** (Northwest Knowledge Network, University of Idaho):

- Variables used: `pr` (precipitation), `pet` (reference ET), `vpd` (vapor pressure deficit),
  `tmmx` (maximum temperature)
- Spatial resolution: ~4 km (1/24°)
- Temporal coverage: 1979–present (daily)
- Reference: Abatzoglou, J.T. (2013). *Development of gridded surface meteorological data for
  ecological applications and modelling.* International Journal of Climatology.
  <https://doi.org/10.1002/joc.3413>

---

## Running Locally (Without Docker)

**Requirements:**

- R 4.4+
- System libraries: `gdal`, `netcdf`, `cdo`
- R packages: `terra`, `ncdf4`, `sf`, `lmomco`, `fs`, `purrr`, `readr`, `tibble`,
  `rnaturalearth`, `rnaturalearthhires`, `gdalUtilities`, `raster`

Set the required environment variables, then run `docker/run_once.sh` or invoke each script
individually. Data writes to `DATA_DIR` (outside the repo by default — not git-tracked):

```bash
export PROJECT_DIR=~/git/mt-climate-office/mco-drought-conus
export DATA_DIR=~/mco-drought-conus-data   # anywhere outside the repo
export CORES=4

bash docker/run_once.sh   # full pipeline

# or run individual scripts:
Rscript R/2_precipitation-metrics.R
Rscript R/3_metrics-spei.R
# etc.
```

S3 sync is skipped automatically when `AWS_BUCKET` is not set.
