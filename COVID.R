# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

library(readr)
library(data.table)
library(tidyverse)

#url_ = "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Casos.csv"
url_ = "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Casos.zip"
url_vacunas = "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/datos_nomivac_covid19.zip"


directorio = "W:\\COVID\\"
setwd(directorio)

salida = "Covid19Casos.zip"
salida_vacunas = "Covid19Vacunas.zip"
salida_csv = "Covid19Casos.csv"
salida_csv_vacunas = "datos_nomivac_covid19.csv"

# download.file(url = url_,
#              destfile = salida,
#              method = 'curl')

download.file(url = url_,
              destfile = salida)

download.file(url = url_vacunas,
              destfile = salida_vacunas)


#salida2 = paste("7z e -so ", salida, sep = "")
#cmd = '7z e -so onefile.Zip'

#casos = fread(cmd = 'unzip -p test/allRequests.csv.zip')


casos = read.csv(unz(salida, salida_csv), header = T)
vacunas = read.csv(unz(salida_vacunas, salida_csv_vacunas), header = T)


#casos = fread(salida2, 
#              encoding = 'UTF-8')

#casos = fread(cmd = salida2)

casos_positivos = casos[which(casos$clasificacion_resumen == "Confirmado"),]

casos_caba = casos[which(casos$residencia_provincia_nombre == "CABA"), ]

columnas_eliminar = c("residencia_departamento_nombre", "sepi_apertura",
                      "fecha_cui_intensivo", "carga_provincia_id",
                      "residencia_provincia_id", "fecha_apertura",
                      "residencia_departamento_id", "ultima_actualizacion")

casos[, columnas_eliminar] = list(NULL)
casos_positivos[, columnas_eliminar] = list(NULL)
casos_caba[, columnas_eliminar] = list(NULL)


columnas_eliminar = c("jurisdiccion_residencia_id", "jurisdiccion_aplicacion_id", "depto_aplicacion_id", "lote_vacuna", "depto_residencia_id")
vacunas[, columnas_eliminar] = list(NULL)
vacunas$Contador = 1

vacunas_agrupado = vacunas %>% group_by(sexo, grupo_etario, jurisdiccion_residencia, depto_residencia, jurisdiccion_aplicacion, depto_aplicacion,
                               fecha_aplicacion, vacuna, condicion_aplicacion, orden_dosis) %>%
                               summarise(Cantidad = sum(Contador))

# Guardo el archivo
file_out = "Casos_COVID_Positivos.txt"
file_out_CABA = "Casos_COVID_CABA.txt"
file_out_TOTAL = "Casos_COVID.txt"
file_out_vacunas = "Vacunas_Covid.txt"

directorio = "W:\\COVID\\"
directorio_out = directorio
setwd(directorio_out)

salida = paste(directorio_out, file_out, sep = "")
salida_CABA = paste(directorio_out, file_out_CABA, sep = "")
salida_TOTAL = paste(directorio_out, file_out_TOTAL, sep = "")
salida_vacunas = paste(directorio_out, file_out_vacunas, sep = "")

fwrite(casos_positivos, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
fwrite(casos_caba, salida_CABA, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
fwrite(casos, salida_TOTAL, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
fwrite(vacunas_agrupado, salida_vacunas, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
library(googlesheets4)
library(lubridate)

gs4_auth(email = "nacho.ls@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "COVID"

status = read_sheet(id_status)
fila = match(codigo_r, status$Script) + 1

if (!is.na(fila)){
  celda = paste("E", fila, sep="")
  
  tryCatch({
    range_write(
      ss = id_status,
      data = df_hora_guardado,
      sheet = 'Status',
      range = celda,
      col_names = FALSE,
      reformat = FALSE
    )
  }, error=function(e){})
}

