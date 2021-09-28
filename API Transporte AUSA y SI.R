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



vclient_id = '19117f54219f451aaebb050195f5e4cd'
vclient_secret = '6e969EA7E03D484dbf164f269F65C33B'

url = 'https://apitransporte.buenosaires.gob.ar/datos/movilidad/transito?'

request = GET(url = url,
              query = list(client_id = vclient_id,
                           client_secret = vclient_secret))

request$status_code

response = content(request, as = "text", encoding = "UTF-8")

df = fromJSON(response) %>% 
  data.frame()

df$hora = ymd_hms(df$hora) - hours(3)
df$Fecha = as.IDate(df$hora)
df$Hora = hour(df$hora)
df$cantidad = as.numeric(df$cantidad)
df$latitud = as.character(df$latitud)
df$longitud = as.character(df$longitud)


# Abro el historico
directorio = "w:\\API_Transporte\\"
setwd(directorio)
file = "Historico_AUSA_SI.txt"

df_historico = fread(file)
df_historico$cantidad = as.numeric(df_historico$cantidad)
df_historico$Hora = as.numeric(df_historico$Hora)

# Sobreescribo los ultimos 7 dias
fecha_corte = as.Date(today() - days(7))

df_historico = df_historico[which(df_historico$Fecha < fecha_corte),]
df = df[which(df$Fecha >= fecha_corte),]


# Uno ambos df
df_out = rbind(df_historico, df)

# Guardo nuevamente el historico
directorio = "w:\\API_Transporte\\"
setwd(directorio)

nombre_salida = file
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(df_out, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t", dec = ",")


## Transporte público ---------------------------------------------------------------------------------------------


url = 'https://apitransporte.buenosaires.gob.ar/datos/movilidad/transportePublico?'

request = GET(url = url,
              query = list(client_id = vclient_id,
                           client_secret = vclient_secret))

request$status_code

response = content(request, as = "text", encoding = "UTF-8")

df = fromJSON(response) %>% 
  data.frame()

df_out=df

directorio = "w:\\API_Transporte\\"
setwd(directorio)

nombre_salida = "test_tp.txt"


directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")


fwrite(df_out, salida, append = TRUE, row.names = FALSE, col.names = FALSE, sep = "\t")



# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "API Transporte AUSA y SI"

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
