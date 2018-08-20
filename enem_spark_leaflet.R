rm(list = ls())
# possiveis erros devido ao encoding do Sistema operacional - OSX...

library(sparklyr)
library(dplyr)
library(leaflet)
library(rgdal)
library(rgeos) 
library(htmlwidgets)
library(readr)
library(ggplot2)
library(maptools)



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
br_simp <- gSimplify(br,tol = 0.008,topologyPreserve=T)
br@polygons <- br_simp@polygons
br@data <- br@data[!is.na(br@data$NOTA_MED),]

# dados do shape file (IBGE) - UF

uf <- readOGR("shapefiles/uf/LIM_Unidade_Federacao_A.shp",layer = "LIM_Unidade_Federacao_A")

uf_simp <- gSimplify(uf,tol = 0.008,topologyPreserve=T)
uf@polygons <- uf_simp@polygons

# Remover simplificacoes ja incorporadas
rm(uf_simp,br_simp)

# Criar a paleta de cores
pal_cidades <- colorNumeric(
  palette = "magma",reverse = T,
  domain = br@data$NOTA_MED)

# Criando o popup
popup_dat_cidades <- paste0("<strong>Cidade: </strong>", 
                    br@data$NOME, 
                    "<br><strong>Estado: </strong>",
                    br@data$SG_UF_RESIDENCIA,
                    "<br><strong>Média das notas: </strong>", 
                    br@data$NOTA_MED,
                    "<br><strong>nº de provas analisadas: </strong>",
                    br@data$count)


# Criar mapa interativo
mapa_cidades <- leaflet(br)%>%
  setView(-50, -20, 4) %>%
  addProviderTiles("Stamen.TonerBackground",
                   options = providerTileOptions(minZoom=4, maxZoom=8))  %>% 
  addPolygons(color = ~pal_cidades(NOTA_MED), 
              weight = 0.75, 
              smoothFactor = 0.9,
              opacity = 0.9, 
              fillOpacity = 0.9,
              highlightOptions = highlightOptions(fillOpacity = 1),
              popup = popup_dat_cidades)%>%
  addPolylines(data =uf ,weight = 1,
               opacity = 1,
               color = "black",
               dashArray = "3",
               highlightOptions = highlightOptions(bringToFront = T)) %>%
  
  addLegend("bottomright", pal = pal_cidades, values = ~NOTA_MED,
            title = "Média ENEM 2017",
            labFormat = labelFormat(prefix = "   "),
            opacity = 1) 

mapa_cidades

# salvar mapa interativo - formato html - 1 minuto
saveWidget(mapa_cidades, file="mapa_enem_cidade.html")


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




# Criar a paleta de cores
pal_estados <- colorNumeric(
  palette = "magma",reverse = T,
  domain = uf@data$NOTA_MED)

# Criando o popup
popup_dat_estados <- paste0("<strong>Cidade: </strong>", 
                            uf@data$NOME, 
                            "<br><strong>Média das notas: </strong>", 
                            uf@data$NOTA_MED,
                            "<br><strong>nº de provas analisadas: </strong>",
                            uf@data$count)


# Criar mapa interativo
mapa_estados <- leaflet(uf)%>%
  setView(-50, -20, 4) %>%
  addProviderTiles("Stamen.TonerBackground",
                   options = providerTileOptions(minZoom=4, maxZoom=8))  %>% 
  addPolygons(fillColor = ~pal_estados(NOTA_MED), 
              weight = 1, 
              smoothFactor = 0.5,
              color = "black",
              fillOpacity = 1,
              highlightOptions = highlightOptions(weight = 2,bringToFront = T),
              popup = popup_dat_cidades)%>%
  
  addLegend("bottomright", pal = pal_estados, values = ~NOTA_MED,
            title = "Média ENEM 2017",
            labFormat = labelFormat(prefix = "   "),
            opacity = 1) 

saveWidget(mapa_estados, file="mapa_enem_estado.html")


dplyr::db_drop_table(sc, "enem_data")
dplyr::db_drop_table(sc, "enem_subset")

