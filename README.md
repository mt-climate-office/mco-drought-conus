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

## Quick Start (Docker)

**Prerequisites:** Docker Desktop (or Docker Engine + Compose plugin).

```bash
# 1. Clone the repo
git clone https://github.com/mt-climate-office/mco-drought-conus.git
cd mco-drought-conus/docker

# 2. Build the image (only needed once, or after Dockerfile changes)
docker compose build

# 3. Run — same command every time, including first run
docker compose up
```

> **Important:** Always run `docker compose` from the `docker/` subdirectory so that the
> `build.context` (`.`) resolves to the directory containing the `Dockerfile`.

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

> **Note on `DATA_DIR`:** `1_gridmet-cache.R` resolves raw download paths relative to
> `~/mco-drought-conus-data` (hardcoded), not from the `DATA_DIR` environment variable.
> Inside Docker this matches the default volume mount so there is no issue. If you customize
> the volume path in `docker-compose.yml`, update the hardcoded path in
> `R/1_gridmet-cache.R` (lines 10–11) to match.

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

Set the required environment variables, then source `docker/run_once.sh` or run each script
individually:

```bash
export PROJECT_DIR=~/mco-drought-conus
export DATA_DIR=~/mco-drought-conus-data
export CORES=4

Rscript R/2_precipitation-metrics.R
Rscript R/3_metrics-spei.R
# etc.
```
