# dowload de arquivos Rodar apenas uma vez

rm(list = ls())

#  instalar pacotes e spark 
#  tutorial instalar spark <- http://spark.rstudio.com


# Download Microdados ENEM - 2017 - salvar na pasta atual

download.file(url = "http://download.inep.gov.br/microdados/microdados_enem2017.zip", 
              destfile = "microdados_enem2017.zip",
              method = "auto", 
              quiet=FALSE)

unzip(zipfile = "microdados_enem2017.zip",overwrite = T)
file.remove("microdados_enem2017.zip")
