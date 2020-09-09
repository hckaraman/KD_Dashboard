# get shiny serves plus tidyverse packages image
FROM rocker/shiny-verse:latest

# system libraries of general use
RUN apt-get update && apt-get install -y \
    sudo \
    pandoc \
    pandoc-citeproc \
    libcurl4-gnutls-dev \
    libcairo2-dev \
    libxt-dev \
    libssl-dev \
    libssh2-1-dev \
    libgdal-dev \
    libproj-dev 

# install R packages required 
# (change it dependeing on the packages you need)
RUN R -e 'install.packages(c("shiny","RSQLite","stringr","SPEI","ggplot2","zoo","sf","rgdal","tidyverse","raster"), repos="http://cran.rstudio.com/")'

# copy the app to the image
COPY KD_Dashboard.Rproj /srv/shiny-server/
COPY server.R /srv/shiny-server/
COPY ui.R /srv/shiny-server/
COPY Data /srv/shiny-server/Data

# select port
EXPOSE 3838

# allow permission
RUN sudo chown -R shiny:shiny /srv/shiny-server

# run app
CMD ["/usr/bin/shiny-server.sh"]