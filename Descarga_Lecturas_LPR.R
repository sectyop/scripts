# Limpio la memoria
rm(list=ls())
gc()


# Cargo librerías
library(httr)
library(jsonlite)
library(tidyverse)
library(leaflet)
library(htmltools)
library(lubridate)
library(tidyr)
library(data.table)
library(googlesheets4)

directorio_out = "W:\\API_Transporte\\Lecturas_LPR\\"
setwd(directorio_out)

web = "http://api.gosit.gcba.gob.ar/movilidad/token"

login = list(
  user = "cgm",
  pass = "K6Z84NW")

posteo = POST(
  url = web,
  body = login,
  encode = c("json")
)

posteo = content(posteo, as = "text", encoding = "UTF-8")
token = fromJSON(posteo) %>% 
  data.frame()

# Obtengo las fechas del último mes
fecha_max = as.Date(today() - 1)
fecha_min = as.Date(make_date(year = year(today()),
                      month = month(today()) - 1,
                      day = day(today()+2)))

# Voy a descargar únicamente la última semana
fecha_corte = fecha_max - 7

dias = as.numeric(fecha_max - fecha_corte)

fechas = c()
flag = 0

# Descargo el .zip fecha por fecha (últ 7d), descomprimo y voy armando un único dataset
for (i in (1:dias)){
  fecha = fecha_corte + i
  web = paste("http://api.gosit.gcba.gob.ar/lpr/detections/", 
              as.character(fecha), sep ="")

  bearer_token = token$token
  
  request = GET(url = web,
                config = add_headers(Authorization = paste("Bearer", bearer_token),
                                     'Accept-Encoding' = "zip"))
  data_temp = request$content
  
  nombre_archivo = paste(as.character(fecha), ".zip", sep = "")
  temp = writeBin(data_temp, nombre_archivo)
  
  nombre_csv = paste("Detections_", as.character(fecha), ".csv", sep = "")
  data_temp = read.table(unz(nombre_archivo, nombre_csv), 
                  sep = ",",
                  header = TRUE)
  if(flag == 0){
    data = data_temp
    flag = 1
  }else{
    data = rbind(data, data_temp)
  }
}

rm("data_temp")  

# Proceso campo datetime
data$timestamp = as_datetime(data$timestamp)
data$fecha = as_date(data$timestamp)
data$hora = hour(data$timestamp)

data_corte = data[which(data$fecha > fecha_corte),]

# Guardo el resultado
directorio_out = "W:\\API_Transporte\\"
setwd(directorio_out)

nombre_salida = "Lecturas_LPR.txt"
salida = paste(directorio_out, nombre_salida, sep = "")


fwrite(data_corte, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Descarga_Lecturas_LPR"

status = read_sheet(id_status)
fila = match(codigo_r, status$Script)+1
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

