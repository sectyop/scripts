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



vclient_id = '19117f54219f451aaebb050195f5e4cd'
vclient_secret = '6e969EA7E03D484dbf164f269F65C33B'

url = 'https://apitransporte.buenosaires.gob.ar/colectivos/vehiclePositionsSimple?'

request = GET(url = url,
              query = list(client_id = vclient_id,
                           client_secret = vclient_secret))

request$status_code

response = content(request, as = "text", encoding = "UTF-8")

df = fromJSON(response) %>% 
  data.frame()

cant_registros = nrow(df)
fecha = data.frame(rep(Sys.Date(), cant_registros))
hora = data.frame(rep(hour(Sys.time())+0, cant_registros))
minuto = data.frame(rep(minute(Sys.time())+0, cant_registros))

df_out_id = cbind(fecha, hora, minuto, df$route_short_name, df$id, df$agency_id, df$agency_name, df$latitude, df$longitude, df$speed)
colnames(df_out_id) = c("Fecha", "Hora","Minuto", "Linea", "Id_Unidad", "Id_Agencia", "Agencia", "Latitud", "Longitud", "Velocidad")

directorio = "N:\\Colectivos\\Txt_5_Min\\"
setwd(directorio)

# -------------------- ABRO LOS ARCHIVOS HISTORICOS PARA ANEXAR LOS DATOS NUEVOS Y QUEDARME CON LOS ULTIMOS 30 DIAS EN DF_OUT_ID

df_out_id_file = "colectivos_acumulado_id_5min.txt"
df_out_id_historico = fread(df_out_id_file)
df_out_id$Fecha = as.IDate(df_out_id$Fecha)

df_out_id_historico = rbind(df_out_id_historico, df_out_id)

fecha_corte = today() - 31
df_out_id_historico = df_out_id_historico[which(df_out_id_historico$Fecha >= fecha_corte),]

# ---------------------------- GUARDO, PERO LOS HISTORICOS DE ID Y SOLO LO NUEVO (ULT 31 DIAS)

nombre_salida_id = "colectivos_acumulado_id_5min.txt"

directorio_out = directorio
salida_id = paste(directorio_out, nombre_salida_id, sep = "")

fwrite(df_out_id_historico, salida_id, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")



# ---------------------------- GUARDO backup mensual si es comienzo de mes



if (day(today()) == 1){
  directorio_out =  "N:\\Colectivos\\Archivos_5min_mensuales\\"
  periodo = format(today()-1, "%Y%m")
  nombre_salida_id = paste(periodo, "_colectivos_acumulado_id_5min.txt", sep = "")
  
  anio_corte = year(today()-1)
  mes_corte = month(today()-1)
  fecha_corte = make_date(anio_corte, mes_corte, 1)
  
  df_out_id_historico = df_out_id_historico[which(df_out_id_historico$Fecha >= fecha_corte),]
  
  salida_id = paste(directorio_out, nombre_salida_id, sep = "")
  
  fwrite(df_out_id_historico, salida_id, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
}


rm(list = ls())


# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
library(googlesheets4)

gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "API_Transporte_Colectivos_5min"

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