# Genero un archivo temporal con servicios aprobados
# Por ahora SON TODOS LOS IDs... esto cambiará cuando haya un proceso

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)
library(googlesheets4)

# Abro el archivo de solicitudes
directorio = "W:\\Colectivos\\Larga_Distancia\\"
setwd(directorio)
file = "Solicitudes_Procesado.txt"
file_ = paste(directorio, file, sep = "")

solicitudes = fread(file_,
                    encoding = 'UTF-8',
                    sep = "\t")


aprobados = as.data.frame(solicitudes$ID)
colnames(aprobados) = c("ID")

# Guardo el archivo de salida
nombre_salida = "servicios_aprobados.txt"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(aprobados, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Solicitudes_TransporteLD_APROBACION"

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
