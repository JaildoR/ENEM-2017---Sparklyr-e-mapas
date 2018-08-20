rm(list = ls())
# possiveis erros devido ao encoding do Sistema operacional - OSX...

library(sparklyr)
library(dplyr)
library(rgdal)
library(rgeos) 
library(readr)
library(ggplot2)
library(maptools)

# baseado https://github.com/eliocamp

LonLabel <- function(lon, east = "\u00B0 E", west = "\u00B0 O") {
lon <- as.numeric(lon)
newlon <- ifelse(lon < 0, paste0(abs(lon), west), paste0(lon, east))
newlon[lon == 0] <- paste0(lon[lon == 0], "\u00B0")
newlon <- ifelse(newlon ==  paste0(0, east),yes = "0\u00B0",no = newlon )

return(newlon)
}

LatLabel <- function(lat, north = "\u00B0 N", south = "\u00B0 S") {
  lat <- as.numeric(lat)
  newlat <- ifelse(lat < 0, paste0(abs(lat), south), paste0(lat, north))
  newlat <- ifelse(newlat == paste0(0, north),yes = "0\u00B0",no = newlat)
  
  return(newlat)
}


##############################
# Ver dicionario <- Importante
#############################

# config do spark --> + memoria

conf <- spark_config()
conf$`sparklyr.shell.driver-memory` <- "16G"  
conf$spark.memory.fraction <- 0.9 
sc <- spark_connect(master = "local", config = conf, version = "2.2.0")


# obter nomes das colunas


enc <- guess_encoding("DADOS/MICRODADOS_ENEM_2017.csv", n_max = 1000)


top_rows <- read.csv("DADOS/MICRODADOS_ENEM_2017.csv", 
                     nrows = 5,
                     header = T,
                     sep = ";",fileEncoding = enc$encoding)

file_columns <- top_rows %>% 
  purrr::map(function(x)"character")

rm(top_rows)


# csv -> spark
enem <- spark_read_csv(sc, 
                       name = "enem_data", 
                       path = "DADOS/MICRODADOS_ENEM_2017.csv", 
                       memory = FALSE, 
                       columns = file_columns, 
                       infer_schema = FALSE,delimiter = ";",charset = enc$encoding)



###### DBI exemplo
library(DBI)

top10 <- dbGetQuery(sc, "Select * from enem_data limit 10")
top10[,1:5]
# OK
rm(top10)

# Criar uma tabela para calculo das medias das notas por cidade 
enem_table <- enem %>%
  mutate(IN_TREINEIRO = as.logical(IN_TREINEIRO),
         TP_PRESENCA_CN = as.numeric(TP_PRESENCA_CN),
         TP_PRESENCA_CH = as.numeric(TP_PRESENCA_CH),
         TP_PRESENCA_LC = as.numeric(TP_PRESENCA_LC),
         TP_PRESENCA_MT = as.numeric(TP_PRESENCA_MT),
         NU_NOTA_CN = as.numeric(NU_NOTA_CN),
         NU_NOTA_CH = as.numeric(NU_NOTA_CH),
         NU_NOTA_LC = as.numeric(NU_NOTA_LC),
         NU_NOTA_MT = as.numeric(NU_NOTA_MT),
         NU_NOTA_REDACAO = as.numeric(NU_NOTA_REDACAO)) %>%
  select(IN_TREINEIRO,TP_PRESENCA_CN,TP_PRESENCA_CH,TP_PRESENCA_LC,
         TP_PRESENCA_MT,CO_MUNICIPIO_RESIDENCIA,SG_UF_RESIDENCIA,
         NU_NOTA_CH,NU_NOTA_CN,NU_NOTA_LC,NU_NOTA_MT, NU_NOTA_REDACAO)

# "gerar tabela no spark" --- demora uns 2 minutos....
subset_table <- enem_table %>% 
  compute("enem_subset")

# Notas por cidade
cidades <- subset_table %>% 
  filter(IN_TREINEIRO == FALSE & 
           TP_PRESENCA_CH == 1 & TP_PRESENCA_CN == 1 &
           TP_PRESENCA_LC == 1 & TP_PRESENCA_MT == 1) %>% 
  group_by(CO_MUNICIPIO_RESIDENCIA,SG_UF_RESIDENCIA) %>% 
  summarise(count = n(),NU_NOTA_CH = mean(NU_NOTA_CH,na.rm = T),
            NU_NOTA_CN = mean(NU_NOTA_CN,na.rm = T),
            NU_NOTA_LC = mean(NU_NOTA_LC,na.rm = T),
            NU_NOTA_MT = mean(NU_NOTA_MT,na.rm = T),
            NU_NOTA_REDACAO = mean(NU_NOTA_REDACAO,na.rm = T)) %>%  as.data.frame()

#calcular media...
cidades$NOTA_MED <- (cidades$NU_NOTA_CH + cidades$NU_NOTA_CN + cidades$NU_NOTA_LC 
                     + cidades$NU_NOTA_MT + cidades$NU_NOTA_REDACAO)/5

# dados do shape file (IBGE) - cidades e uf
# Bcim - LIM 2016
# Disponivel em: https://www.ibge.gov.br/geociencias-novoportal/cartas-e-mapas/...
# ...bases-cartograficas-continuas/15759-brasil.html?=&t=downloads
br <- readOGR("shapefiles/municipios/LIM_Municipio_A.shp",layer = "LIM_Municipio_A")

br@data$GEOCODIGO <- as.character(br@data$GEOCODIGO)

# inserir dados do ENEM nos dados do shapefile 
br@data <- left_join(x = br@data,y = cidades,by = c("GEOCODIGO" = "CO_MUNICIPIO_RESIDENCIA"))
br@data$NOME <- as.character(br@data$NOME)

br@data$NOTA_MED <- round(br@data$NOTA_MED,digits = 1)

# simplificar poligonos /problemas com memoria 
br_simp <- gSimplify(br,tol = 0.025,topologyPreserve=T)
br@polygons <- br_simp@polygons
br@data <- br@data[!is.na(br@data$NOTA_MED),]

# dados do shape file (IBGE) - UF

uf <- readOGR("shapefiles/uf/LIM_Unidade_Federacao_A.shp",layer = "LIM_Unidade_Federacao_A")

uf_simp <- gSimplify(uf,tol = 0.025,topologyPreserve=T)
uf@polygons <- uf_simp@polygons

# Remover simplificacoes ja incorporadas
rm(uf_simp,br_simp)

####################

br@data <- br@data %>% mutate(id = row.names(.))
br_df <- broom::tidy(br, region = "id")
br_df <- br_df %>% left_join(br@data, by = c("id"="id"))


#

map <- ggplot() +  
  geom_polygon(data = br_df, 
               aes(x = long, y = lat,group = group,fill = NOTA_MED,colour = NOTA_MED)) + 
  scale_fill_viridis_c(option = "magma", direction = -1,
                       name = "Média Notas ENEM 2017",
                       guide = guide_colorbar(
                         direction = "horizontal",
                         barheight = unit(2, units = "mm"),
                         barwidth = unit(75, units = "mm"),
                         draw.ulim = F,
                         title.position = 'top',
                         # some shifting around
                         title.hjust = 0.5,
                         label.hjust = 0.5)) +
  scale_colour_viridis_c(option = "magma", direction = -1,guide = FALSE) +
  geom_path(data = uf, aes(long, lat, group = group),color ="white", size = 0.4) +
  scale_x_continuous(labels = LonLabel) +
  scale_y_continuous(labels = LatLabel) +
  theme(plot.background = element_rect(fill = "snow2"),
        panel.background = element_rect(fill = "transparent",
                                        colour = "transparent"),
        legend.background =element_rect(fill = "transparent"),
        panel.grid.minor = element_blank(),
        panel.grid.major = element_line(color="#e1d7d7", size=0.5),
        legend.position = "bottom",
        axis.ticks.x=element_blank(),
        axis.ticks.y=element_blank(),
        plot.margin = unit(c(.5,.5,.25,.5), "cm"),
        text = element_text(family="Helvetica Neue Thin"),
        legend.text = element_text(size = 10, hjust = 0, color = "#2e2e2a"),
        plot.title = element_text(size = 14,hjust = 0, color = "#2e2e2a",
                                  margin = margin(b = 0.1, 
                                                  t = 0,
                                                  r = 0,
                                                  l = 1, 
                                                  unit = "cm")),
        plot.subtitle = element_text(size = 12,hjust = 0, color = "#2e2e2a", 
                                     margin = margin(b = 0.6, 
                                                     t = 0,
                                                     r =0,
                                                     l = 1, 
                                                     unit = "cm")),
        legend.title = element_text(size = 11))+ 
  coord_equal() +
  labs(x = NULL, 
       y = NULL, 
       title = "ENEM 2017:", 
       subtitle = "Média das notas por cidade", 
       caption = "Base Cartográfica: IBGE, 2018; Data: INEP, 2018")

map

# Based on https://timogrossenbacher.ch/2016/12/beautiful-thematic-maps-with-ggplot2-only/

saveRDS(map,file = "mapa_enem_cidade_ggplot.rds")




# mapa_notas por estado


# Notas por cidade
estados <- subset_table %>% 
  filter(IN_TREINEIRO == FALSE & 
           TP_PRESENCA_CH == 1 & TP_PRESENCA_CN == 1 &
           TP_PRESENCA_LC == 1 & TP_PRESENCA_MT == 1) %>% 
  group_by(SG_UF_RESIDENCIA) %>% 
  summarise(count = n(),NU_NOTA_CH = mean(NU_NOTA_CH,na.rm = T),
            NU_NOTA_CN = mean(NU_NOTA_CN,na.rm = T),
            NU_NOTA_LC = mean(NU_NOTA_LC,na.rm = T),
            NU_NOTA_MT = mean(NU_NOTA_MT,na.rm = T),
            NU_NOTA_REDACAO = mean(NU_NOTA_REDACAO,na.rm = T)) %>%  as.data.frame()

estados$NOTA_MED <- (estados$NU_NOTA_CH + estados$NU_NOTA_CN + estados$NU_NOTA_LC 
                     + estados$NU_NOTA_MT + estados$NU_NOTA_REDACAO)/5


uf@data$SIGLA <- as.character(uf@data$SIGLA)


uf@data <- left_join(x = uf@data,y = estados,by = c("SIGLA" = "SG_UF_RESIDENCIA"))
uf@data$NOME <- as.character(uf@data$NOME)

uf@data$NOTA_MED <- round(uf@data$NOTA_MED,digits = 1)

uf@data <- uf@data %>% mutate(id = row.names(.))
uf_df <- broom::tidy(uf, region = "id")
uf_df <- uf_df %>% left_join(uf@data, by = c("id"="id"))






map_estado <- ggplot() +  
  geom_polygon(data = uf_df, 
               aes(x = long, y = lat,group = group,fill = NOTA_MED),colour = "white",size = 0.4) + 
  scale_fill_viridis_c(option = "magma", direction = -1,
                       name = "Média Notas ENEM 2017",
                       guide = guide_colorbar(
                         direction = "horizontal",
                         barheight = unit(2, units = "mm"),
                         barwidth = unit(75, units = "mm"),
                         draw.ulim = F,
                         title.position = 'top',
                         # some shifting around
                         title.hjust = 0.5,
                         label.hjust = 0.5)) +
  scale_x_continuous(labels = LonLabel) +
  scale_y_continuous(labels = LatLabel) +
  theme(plot.background = element_rect(fill = "snow2"),
        panel.background = element_rect(fill = "transparent",
                                        colour = "transparent"),
        legend.background =element_rect(fill = "transparent"),
        panel.grid.minor = element_blank(),
        panel.grid.major = element_line(color="#e1d7d7", size=0.5),
        legend.position = "bottom",
        axis.ticks.x=element_blank(),
        axis.ticks.y=element_blank(),
        plot.margin = unit(c(.5,.5,.25,.5), "cm"),
        text = element_text(family="Helvetica Neue Thin"),
        legend.text = element_text(size = 10, hjust = 0, color = "#2e2e2a"),
        plot.title = element_text(size = 14,hjust = 0, color = "#2e2e2a",
                                  margin = margin(b = 0.1, 
                                                  t = 0,
                                                  r = 0,
                                                  l = 1, 
                                                  unit = "cm")),
        plot.subtitle = element_text(size = 12,hjust = 0, color = "#2e2e2a", 
                                     margin = margin(b = 0.6, 
                                                     t = 0,
                                                     r =0,
                                                     l = 1, 
                                                     unit = "cm")),
        legend.title = element_text(size = 11))+ 
  coord_equal() +
  labs(x = NULL, 
       y = NULL, 
       title = "ENEM 2017:", 
       subtitle = "Média das notas por estado", 
       caption = "Base Cartográfica: IBGE, 2018; Data: INEP, 2018")

map_estado

saveRDS(map_estado,file = "mapa_enem_estado_ggplot.rds")




dplyr::db_drop_table(sc, "enem_data")
dplyr::db_drop_table(sc, "enem_subset")