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

RUN R -q -e 'install.packages(c("fs","purrr","readr","tibble","rnaturalearth","gdalUtilities","lmomco","raster","ncdf4","pak","pbmcapply"), repos="https://cloud.r-project.org")'

RUN R -q -e 'pak::pkg_install("ropensci/rnaturalearthhires")'

# Create non-root user for running the pipeline
RUN useradd -m -s /bin/bash mco-drought

WORKDIR /opt/app

# Bake R scripts into the image for Fargate (no volume mount available).
# Local docker-compose runs override this via the source volume mount.
COPY R/ /opt/app/R/
COPY docker/run_once.sh /opt/app/run_once.sh
COPY docker/make_web_cogs.sh /opt/app/docker/make_web_cogs.sh
COPY docker/run_test.sh /opt/app/docker/run_test.sh
RUN chmod +x /opt/app/run_once.sh /opt/app/docker/make_web_cogs.sh /opt/app/docker/run_test.sh

CMD ["/opt/app/run_once.sh"]
