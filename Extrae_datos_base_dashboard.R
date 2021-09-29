# Limpio la memoria
rm(list=ls())
gc()


# Cargo librerías
library(openxlsx)
library(data.table)
library(dplyr)
library(RCurl)
library(googlesheets4)
library(lubridate)


directorio = "w:\\COVID\\"
xlsxFile = "Base Dashboard.xlsx"
hoja = "Data"
setwd(directorio)


df = read.xlsx(
              xlsxFile,
              sheet = hoja,
              startRow = 1,
              colNames = TRUE,
              rowNames = FALSE,
              detectDates = FALSE,
              skipEmptyRows = TRUE,
              skipEmptyCols = TRUE,
              rows = NULL,
              cols = NULL,
              check.names = FALSE,
              sep.names = ".",
              namedRegion = NULL,
              na.strings = "NA",
              fillMergedCells = FALSE
)

df = df[which(df$Guardar_API == 1),]
df$Fecha = as.Date(df$Fecha, origin = "1899-12-30")

df$Guardar_API = NULL
df$Día.Hábil = NULL
df$Color = NULL
df$Semana_Max = NULL
df$Semana = NULL


df2 = df[which(df$Semana_Informe == "Sem Ref"),]

df$concat = paste(df$Medio, df$Detalle, df$Día_Semana)
df2$concat = paste(df2$Medio, df2$Detalle, df2$Día_Semana)

df3 = merge(df, df2, by.x = "concat", by.y = "concat", all.x = TRUE)

df3$Variacion = round(df3$Pasajeros.x / df3$Pasajeros.y - 1, 4)

columnas_guardar = c("Fecha.x", "Medio.x", "Detalle.x", "Pasajeros.x", "Variacion")

df3 = df3[, columnas_guardar]
columnas_nombres = c("Fecha", "Medio", "Detalle", "Cantidad", "Variacion")
colnames(df3) = columnas_nombres

df3 = df3[order(df3$Fecha, df3$Medio, df3$Detalle),]

df3 = df3[which(df3$Fecha >= "2020-03-09"),]
df3$Cantidad = round(df3$Cantidad,0)                  



# GUARDO LA PRIMERA SALIDA CON EL DESAGREGADO DE TODOS LOS MEDIOS
nombre_salida = "datos_transporte.txt"

directorio_out = "w:\\API_Transporte\\"
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(df3, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# Elimino TomTom y Sensores Internos

df3 = df3[which(df3$Detalle != "Sensores Internos" & df3$Detalle != "TomTom"),]

# PREPARO UN NUEVO DF CON TODOS LOS MEDIOS AGREGADOS Y SUMARIZADOS
df4 = df3 %>%
  group_by(Fecha, Medio) %>%
  summarise(Cantidad_ = sum(Cantidad))

df4 = df4[which(df4$Medio != "Colectivos OD"),]

df4$Dia_Semana = weekdays(df4$Fecha)
df4$concat = paste(df4$Medio, df4$Dia_Semana)


# -------- separo la semana de referencia para volver a armar la comparación

sem_ref = df4[which(df4$Fecha <= "2020-03-15"),]

df4 = merge(df4, sem_ref, by.x = "concat", by.y = "concat", all.x = TRUE)

df4$Variacion = round(df4$Cantidad_.x / df4$Cantidad_.y - 1 , 4)

columnas_guardar = c("Fecha.x", "Medio.x", "Cantidad_.x", "Variacion")

df4 = df4[, columnas_guardar]
columnas_nombres = c("Fecha", "Medio", "Cantidad", "Variacion")
colnames(df4) = columnas_nombres

df4 = df4[order(df4$Fecha, df4$Medio),]

df4$Cantidad = round(df4$Cantidad,0)                  

# ----------- GUARDO LA SEGUNDA SALIDA CON EL AGREGADO DE TODOS LOS MEDIOS
nombre_salida = "datos_transporte_agregados.txt"

directorio_out = "w:\\API_Transporte\\"
setwd(directorio_out)
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(df4, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# ----------- Subo el archivo al ftp server

local_file = salida
usuario = "jlopezsaez"
password = "Jl0pezs4ez"
host = "ftp.buenosaires.gob.ar/input_transporte_web_covid"
port = 21
destino = paste("ftp://",usuario,":",password,"@",host,"/",nombre_salida, sep ="")

ftpUpload(what = local_file, to = destino)
#ftpUpload(what = local_file, to = "ftp://jlopezsaez:Jl0pezs4ez@ftp.buenosaires.gob.ar/input_transporte_web_covid/datos_transporte.txt")




# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "nacho.ls@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Extrae_datos_base_dashboard"

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
