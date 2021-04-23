library(tidyr)
library(tidyverse)
library(pivottabler)
library(vroom)
library(ggplot2)
library(hrbrthemes)
library(sf)
library("rnaturalearth")
library("rnaturalearthdata")
library(ggplot2)
library("ggspatial")
library(raster)
require(rasterVis)
library(stars)
library(ggpubr)
library(stringr)


df <- readxl::read_excel('/mnt/v1/GTE/Data/tmp3/stations.xlsx')
df$Zaman <- as.Date(df$Zaman)

stations_name <- unique(df$ist_name) 
stations_no <-  unique(df$ist_no) 

i <- 1

for (station in stations_no) {
  df.t <- df %>% filter(ist_no == station) %>% pivot_longer(cols = c('Sıcaklık','LST Gece','LST Gunduz'))
  
  ylab = expression(paste("Sıcaklık, ", degree, "C"))
  xlab = "Tarih"
  title  <- paste("Hava ve Yüzey Sıcaklığı : ",stations_name[i],sep='')
  
  g <- ggplot(df.t) +
    aes(x = Zaman, y = value, colour = name,size=1L) +
    geom_line(size = 1) +
    geom_point(size = 2L) + 
    scale_color_hue(direction = 1) + 
    labs(title=title, x = xlab , y = ylab) +
    theme(
      plot.title = element_text(size = 26),
      axis.text = element_text(size = 26),
      axis.title = element_text(size = 26),
      legend.title = element_blank(),
      legend.text = element_text(size = 16),
      # legend.position = "none"
      # legend.position=c(.12,.80),
      # legend.background = element_rect(fill="transparent")
    ) 
  
  file <- str_c("/mnt/v1/GTE/Data/tmp3/pics/", station,".png")
  ggsave(file, plot = g, width = 40, height = 20, units = "cm", dpi = 300)
  
  i <- i + 1
}


