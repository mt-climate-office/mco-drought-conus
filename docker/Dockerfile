FROM rocker/geospatial:4.4.1

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    cdo netcdf-bin libnetcdf-dev gdal-bin curl ca-certificates tzdata \
  && rm -rf /var/lib/apt/lists/*

ENV TZ=America/Denver
ENV OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1

RUN R -q -e 'install.packages(c("fs","purrr","readr","tibble","rnaturalearth","gdalUtilities","lmomco","raster","ncdf4","pak","pbmcapply"), repos="https://cloud.r-project.org")'

RUN R -q -e 'pak::pkg_install("ropensci/rnaturalearthhires")'

WORKDIR /opt/app
COPY run_once.sh /opt/app/run_once.sh
RUN chmod +x /opt/app/run_once.sh

CMD ["/opt/app/run_once.sh"]
