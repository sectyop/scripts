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

# vclient_id: 'ed9986ba27914bed828c8a5f6faf681c'
# vclient_secret:'5197800F64f04910a83091B8795549D5'

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

df_out = cbind(fecha, hora, df$route_short_name, df$agency_id, df$agency_name)
colnames(df_out) = c("Fecha", "Hora", "Linea", "Id_Agencia", "Agencia")

df_out_id = cbind(fecha, hora, df$route_short_name, df$id, df$agency_id, df$agency_name, df$latitude, df$longitude)
colnames(df_out_id) = c("Fecha", "Hora", "Linea", "Id_Unidad", "Id_Agencia", "Agencia", "Latitud", "Longitud")

df_out_id_2 = cbind(fecha, hora, df$route_id, df$trip_headsign,df$route_short_name, df$id, df$agency_id, df$agency_name, df$latitude, df$longitude)
colnames(df_out_id_2) = c("Fecha", "Hora", "Route_ID", "Route_HS", "Linea", "Id_Unidad", "Id_Agencia", "Agencia", "Latitud", "Longitud")

df_out = df_out %>%
  group_by(Fecha, Hora, Linea, Id_Agencia, Agencia) %>%
  summarise(count=n())

df_out_id = df_out_id %>%
  group_by(Fecha, Hora, Linea, Id_Unidad, Id_Agencia, Agencia, Latitud, Longitud) %>%
  summarise(count=n())

df_out_id_2 = df_out_id_2 %>%
  group_by(Fecha, Hora, Route_ID, Route_HS, Linea, Id_Unidad, Id_Agencia, Agencia, Latitud, Longitud) %>%
  summarise(count=n())

colnames(df_out) = c("Fecha", "Hora", "Linea", "Id_Agencia", "Agencia", "Cantidad")
colnames(df_out_id) = c("Fecha", "Hora", "Linea", "Id_Unidad", "Id_Agencia", "Agencia","Latitud", "Longitud", "Cantidad")
colnames(df_out_id_2) = c("Fecha", "Hora", "Route_ID", "Route_HS", "Linea", "Id_Unidad", "Id_Agencia", "Agencia","Latitud", "Longitud", "Cantidad")

directorio = "w:\\Colectivos\\"
setwd(directorio)

nombre_salida = "colectivos_acumulado.txt"
nombre_salida_id = "colectivos_acumulado_id.txt"
nombre_salida_id2 = "colectivos_acumulado_id2.txt"

directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")
salida_id = paste(directorio_out, nombre_salida_id, sep = "")
salida_id2 = paste(directorio_out, nombre_salida_id2, sep = "")


# -------------------- ABRO LOS ARCHIVOS HISTORICOS PARA ANEXAR LOS DATOS NUEVOS Y QUEDARME CON LOS ULTIMOS 60 DIAS EN DF_OUT_ID Y DF_OUT_ID_2

df_out_id_file = "colectivos_acumulado_id.txt"
df_out_id_historico = fread(df_out_id_file)
df_out_id$Fecha = as.IDate(df_out_id$Fecha)

df_out_id_2_file = "colectivos_acumulado_id2.txt"
df_out_id_2_historico = fread(df_out_id_2_file)
df_out_id_2$Fecha = as.IDate(df_out_id_2$Fecha)

df_out_id_historico = rbind(df_out_id_historico, df_out_id)
df_out_id_2_historico = rbind(df_out_id_2_historico, df_out_id_2)

fecha_corte = today() - 60
df_out_id_historico = df_out_id_historico[which(df_out_id_historico$Fecha >= fecha_corte),]
df_out_id_2_historico = df_out_id_2_historico[which(df_out_id_2_historico$Fecha >= fecha_corte),]

# ---------------------------- GUARDO, PERO LOS HISTORICOS DE ID Y ID_2 SOLO LO NUEVO (ULT 60 DIAS)

fwrite(df_out, salida, append = TRUE, row.names = FALSE, col.names = FALSE, sep = "\t")
fwrite(df_out_id_historico, salida_id, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
fwrite(df_out_id_2_historico, salida_id2, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

rm(list = ls())


# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
library(googlesheets4)

gs4_auth(email = "nacho.ls@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "API_Transporte_Colectivos"

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


