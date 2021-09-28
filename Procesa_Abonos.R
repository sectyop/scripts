# ------------------------------------------------------------------------------------
# 22/04/2021
# Tengo que procesar un poco el archivo de abonos pq cada 10 minutos me cambian las columnas y los nombres de las mismas
# Y es un bardo siempre pq tira error en PowerBI
# ------------------------------------------------------------------------------------

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)
library(readxl)
library(googlesheets4)

# Abro el archivo

directorio = "W:\\Ecobici\\Bajadas\\Abonos\\"

setwd(directorio)

#file = "Abonos.xlsx"
file = "Abonos.csv"

abonos = fread(file,
               encoding = 'UTF-8')

# Corrigo valores que viene con pésimo encoding
#table(abonos$`Plan Name`)
abonos$`Plan Name`[abonos$`Plan Name` == "viaje Ãºnico"] = "viaje único"
abonos$`Plan Name`[abonos$`Plan Name` == "bÃ¡sico"] = "basico"

#abonos = read_xlsx(
#  file,
#  #sheet = "Conteo",
#  col_names = TRUE)

colnames(abonos) = c("Fecha_Compra", "Abono", "Origen", "Cantidad", "Facturacion")
#abonos$Fecha_Compra = ymd_hms(abonos$Fecha_Compra)
abonos$Precio_Promedio = abonos$Facturacion / abonos$Cantidad
abonos$Tipo_Compra = "Compra"

# Veo la info de renovaciones
file = "Autorenovacion.xlsx"

renovaciones = read_xlsx(
  file,
  #sheet = "Conteo",
  col_names = TRUE)

#colnames(renovaciones)
colnames(renovaciones) = c("Fecha_Compra", "Abono", "Cantidad", "Facturacion")
renovaciones$Fecha_Compra = ymd_hms(renovaciones$Fecha_Compra)
renovaciones$Facturacion = as.numeric(renovaciones$Facturacion)
renovaciones$Precio_Promedio = renovaciones$Facturacion / renovaciones$Cantidad
renovaciones$Tipo_Compra = "Renovacion"
renovaciones$Origen = "WEB"

abonos = rbind(abonos, renovaciones)

# Guardo el archivo de salida
nombre_salida = "Abonos_Procesado.txt"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(abonos, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")




# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Procesa_Abonos"

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
