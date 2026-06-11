FROM rocker/geospatial:4.4.1

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    cdo netcdf-bin libnetcdf-dev gdal-bin curl ca-certificates tzdata unzip \
  && rm -rf /var/lib/apt/lists/*

# AWS CLI v2 (needed for S3 sync of derived outputs)
RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip \
  && unzip -q /tmp/awscliv2.zip -d /tmp \
  && /tmp/aws/install \
  && rm -rf /tmp/awscliv2.zip /tmp/aws

ENV TZ=America/Denver
ENV OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1

# CRAN pinned to a dated Posit snapshot so rebuilds are reproducible —
# floating "latest CRAN" silently bumped terra 1.9.11 -> 1.9.27 on a rebuild
# and broke the pipeline. terra is installed explicitly so its version comes
# from the snapshot, not whatever the base image shipped. Bump the date
# deliberately when upgrading packages.
ARG CRAN_SNAPSHOT=2026-06-10
RUN R -q -e 'install.packages(c("terra","fs","purrr","readr","tibble","rnaturalearth","gdalUtilities","lmomco","raster","ncdf4","pbmcapply"), repos=sprintf("https://packagemanager.posit.co/cran/__linux__/jammy/%s", Sys.getenv("CRAN_SNAPSHOT", "2026-06-10")))'

# rnaturalearthhires is too large for CRAN; install from rOpenSci r-universe.
# (A GitHub install via pak hits unauthenticated API rate limits on CI runners.)
RUN R -q -e 'install.packages("rnaturalearthhires", repos=c("https://ropensci.r-universe.dev", "https://cloud.r-project.org"))'

# Create non-root user for running the pipeline
RUN useradd -m -s /bin/bash mco-drought

WORKDIR /opt/app

# Bake R scripts into the image for Fargate (no volume mount available).
# Local docker-compose runs override this via the source volume mount.
COPY R/ /opt/app/R/
COPY pipeline/run_once.sh /opt/app/run_once.sh
COPY pipeline/make_web_cogs.sh /opt/app/pipeline/make_web_cogs.sh
COPY pipeline/run_test.sh /opt/app/pipeline/run_test.sh
RUN chmod +x /opt/app/run_once.sh /opt/app/pipeline/make_web_cogs.sh /opt/app/pipeline/run_test.sh

CMD ["/opt/app/run_once.sh"]
