# Limpio memoria del sistema
rm(list = ls())
gc()
Sys.time()

# Cargo librerias
library(googledrive)
library(googlesheets4)
library(lubridate)
#library(dplyr)
#library(xlsx)
library(readxl)
library(data.table)
library(tidyverse)

# Busco archivos con el nombre de 1. Reporte Alcoholemia.xlsx - Esto lo uso una sola vez, para buscar el ID y despues lo comento asi no lo busca siempre

# Obtengo el ID del que quiero utilizar

drive_auth(email = "datos.ssgm@gmail.com")

id_alcoholemia = drive_get("Planilla Alcoholemia")
id_alcoholemia = id_alcoholemia$id

archivo = as_id(id_alcoholemia)
nombre_archivo = "Reporte Alcoholemia_GoogleSheets.xlsx"

# Descargo el archivo
directorio = "W:\\Agentes Viales\\Alcoholemia\\"
path_ = paste(directorio, nombre_archivo, sep = "")

drive_download(
  file = archivo,
  path = path_,
  overwrite = TRUE)

# Leo el archivo y guardo la pestaña de "datos puestos"
setwd(directorio)
data_puestos = read_xlsx(
  nombre_archivo,
  sheet = "Datos Puestos",
  col_names = TRUE,
  .name_repair = "unique")

## Corrijo todos los campos de fecha y hora
data_puestos$Fecha = as.IDate(data_puestos$Fecha)

if (class(data_puestos$`Hora llegada`)[1] == "character"){
  data_puestos$`Hora llegada` =  as.numeric(data_puestos$`Hora llegada`)
}else{
  data_puestos$`Hora llegada` = hour(data_puestos$`Hora llegada`)/24 + minute(data_puestos$`Hora llegada`)/(24*60)
}

if (class(data_puestos$`Hora Inicio`)[1] == "character"){
  data_puestos$`Hora Inicio` =  as.numeric(data_puestos$`Hora Inicio`)
}else{
  data_puestos$`Hora Inicio` = hour(data_puestos$`Hora Inicio`)/24 + minute(data_puestos$`Hora Inicio`)/(24*60)
}

if (class(data_puestos$`Hora Fin`)[1] == "character"){
  data_puestos$`Hora Fin` =  as.numeric(data_puestos$`Hora Fin`)
}else{
  data_puestos$`Hora Fin` = hour(data_puestos$`Hora Fin`)/24 + minute(data_puestos$`Hora Fin`)/(24*60)
}

## Genero los campos de ubicación final, lat y lon

data_puestos$Ubicacion_Final = ifelse(data_puestos$`Calle 1` < data_puestos$`Calle 2`,
                                      paste(data_puestos$`Calle 1`, " y ", data_puestos$`Calle 2`, sep = ""),
                                      paste(data_puestos$`Calle 2`, " y ", data_puestos$`Calle 1`, sep = ""))

urls = data_puestos$`Link Google Maps`

#urls
Arroba = str_locate(data_puestos$`Link Google Maps`, "@")
Arroba = Arroba[, 1]
data_puestos = cbind(data_puestos, Arroba)

Sub_Link = substr(data_puestos$`Link Google Maps`, data_puestos$Arroba, data_puestos$Arroba + 30)
#Sub_Link
Coma = str_locate(Sub_Link, ",")
Coma = Coma[, 1]
#Coma
data_puestos = cbind(data_puestos, Coma)

data_puestos$Lat = substr(data_puestos$`Link Google Maps`, 
                          start = data_puestos$Arroba + 1, 
                          stop = data_puestos$Arroba + data_puestos$Coma - 2)
data_puestos$Lat = as.numeric(data_puestos$Lat)
#$Lat

zeta = str_locate(Sub_Link, "z")
zeta = zeta[ , 1]
zeta_min = min(zeta, na.rm = TRUE)
#zeta
#zeta_min

data_puestos = cbind(data_puestos, zeta)
data_puestos$zeta = zeta_min

data_puestos$Lon = substr(data_puestos$`Link Google Maps`, 
                          start = data_puestos$Arroba + data_puestos$Coma, 
                          stop = data_puestos$Arroba + data_puestos$zeta - 5)

data_puestos$Lon = as.numeric(data_puestos$Lon)
#data_puestos$Lon
data_puestos$Lat = round(data_puestos$Lat, 4)
data_puestos$Lon = round(data_puestos$Lon, 4)

data_puestos$Arroba = NULL
data_puestos$Coma = NULL
data_puestos$zeta = NULL
#data_puestos$Lat
#data_puestos$Lon

# Leo el archivo de nuevo y guardo la pestaña de "datos dia"
data_dia = read_xlsx(
  nombre_archivo,
  sheet = "Datos Día",
  col_names = TRUE,
  .name_repair = "unique")

## Corrijo todos los campos de fecha y hora
data_dia$Fecha = as.IDate(data_dia$Fecha)

# Leo los archivos historicos
# ---------------------------------- Aca genero los txt a partir del excel actual------------
# nombre_archivo = "1. Reporte Alcoholemia_Historico.xlsx"
# 
# historico_puestos = read_xlsx(
#   nombre_archivo,
#   sheet = "Datos Puestos",
#   col_names = TRUE,
#   #skip = 3,
#   .name_repair = "unique")
# 
# historico_dia = read_xlsx(
#   nombre_archivo,
#   sheet = "Datos Dia",
#   col_names = TRUE,
#   #skip = 2,
#   .name_repair = "unique")
# 
# # colnames(historico_puestos)
# # colnames(data_puestos)
# 
# #historico_puestos = historico_puestos[,1:48]
# historico_puestos = historico_puestos[which(!is.na(historico_puestos$Fecha)),]
# 
# historico_dia = historico_dia[,1:7]
# historico_dia = historico_dia[which(!is.na(historico_dia$Fecha)),]
# 
# file_out = "historico_puestos.txt"
# directorio_out = directorio
# setwd(directorio_out)
# 
# salida = paste(directorio_out, file_out, sep = "")
# 
# fwrite(historico_puestos, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
# 
# file_out = "historico_puestos_dia.txt"
# directorio_out = directorio
# setwd(directorio_out)
# 
# salida = paste(directorio_out, file_out, sep = "")
# 
# fwrite(historico_dia, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
# ----------------------------------------------------------------------------------

directorio = "W:\\Agentes Viales\\Alcoholemia\\"
nombre_historico_puestos = "historico_puestos.txt"
path_historico_puestos = paste(directorio, nombre_historico_puestos, sep = "")

nombre_historico_puestos_dia = "historico_puestos_dia.txt"
path_historico_puestos_dia = paste(directorio, nombre_historico_puestos_dia, sep = "")


historico_puestos = fread(path_historico_puestos,
                          header = TRUE, 
                          sep = "\t", 
                          encoding = 'UTF-8')

historico_dia = fread(path_historico_puestos_dia,
                      header = TRUE, 
                      sep = "\t", 
                      encoding = 'UTF-8')

## Corrijo todos los campos de fecha y hora
historico_puestos$Fecha = as.IDate(historico_puestos$Fecha)
historico_puestos$`Hora llegada` = as.numeric(historico_puestos$`Hora llegada`)
historico_puestos$`Hora Inicio` = as.numeric(historico_puestos$`Hora Inicio`)
historico_puestos$`Hora Fin` = as.numeric(historico_puestos$`Hora Fin`)

historico_dia$Fecha = as.IDate(historico_dia$Fecha)

nombres_puestos = colnames(data_puestos)
colnames(historico_puestos) = nombres_puestos
nombres_dia = colnames(data_dia)
colnames(historico_dia) = nombres_dia

consolidado_historico = rbind(historico_puestos, data_puestos)
consolidado_historico = unique(consolidado_historico)

consolidado_dia = rbind(historico_dia, data_dia)
consolidado_dia = unique(consolidado_dia)


# Guardo los archivos de salida

file_out = "Bajada_Alcoholemia.csv"

directorio_out = directorio
setwd(directorio_out)

salida = paste(directorio_out, file_out, sep = "")

fwrite(consolidado_historico, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = ",")

file_out = "Bajada_Datos_Dia.csv"

directorio_out = directorio
setwd(directorio_out)

salida = paste(directorio_out, file_out, sep = "")

fwrite(consolidado_dia, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = ",")

### ---- 11/12/2020 NLS --> Veo si hay nuevas ubicaciones
# 
# ubicaciones = unique(data_puestos$...6)
# ubicaciones = as.data.frame(ubicaciones[3:length(ubicaciones)])
# colnames(ubicaciones) = c("Ubicacion")
# 
# # ---- Leo el archivo de ubicaciones
# directorio = "W:\\Agentes Viales\\Alcoholemia\\Ubicaciones\\"
# file = "Ubicaciones_para_trabajar.xlsx"
# archivo = paste(directorio,file, sep ="")
# 
# ref_ubicaciones = read_xlsx(
#   archivo,
#   sheet = "ubicaciones_final",
#   col_names = TRUE,
#   .name_repair = "unique")
# 
# #ref_ubicaciones = as.data.frame(unique(ref_ubicaciones$`Ubicacion original`))
# #colnames(ref_ubicaciones) = c("ref_ubicacion")
# 
# ubicaciones = merge(ubicaciones, ref_ubicaciones, by.x = "Ubicacion", by.y = "Ubicacion original" ,  all.x = TRUE)
# ubicaciones_nuevas = ubicaciones[which(is.na(ubicaciones$Ubicacion_Corregida)),]
# ubicaciones_nuevas = as.data.frame(unique(ubicaciones_nuevas$Ubicacion))
# 
# file_out = "Ubicaciones_NUEVAS.txt"
# archivo_salida = paste(directorio,file_out, sep = "")
# 
# fwrite(ubicaciones_nuevas, archivo_salida)



# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Alcoholemia_GoogleSheets"

status = read_sheet(id_status)
fila = match(codigo_r, status$Script)+1
celda = paste("E", fila, sep="")


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

