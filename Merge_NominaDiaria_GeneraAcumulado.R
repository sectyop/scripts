#------------------------------------------------------------------------------------------
# DB Nomina CAT
# 23/02/2021
# Se va guardando un txt con la "foto" de la nomina cada vez que se hace el update de presentismo
# En este codigo, haremos un acumulado con todas esas fotos, cosa que en el BI se puedan comparar
# 2 "fotos" de distintos días
#------------------------------------------------------------------------------------------

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)
#library(RPostgreSQL)
#library(DBI)
#library(RPostgres)
library(stringr)
library(googlesheets4)
library(gtools)


# --------------------------------------- MERGE ARCHIVOS MENSUALES

directorio = "W:\\Agentes Viales\\Nomina\\Bajadas_Nomina\\"
directorio_out = "W:\\Agentes Viales\\Nomina\\"
setwd(directorio)

Allfiles = list.files(pattern = "\\.txt$")

dias_ventana = 90
fecha_corte_merge = as.Date(today()) - dias_ventana

vuelta = 1

Sys.time()
for (file in Allfiles){
  if(vuelta < 2){
    data_merge = fread(file, 
                       header = TRUE, 
                       sep = "\t", 
                       encoding = 'UTF-8',
                       #select = columnas_importar,
                       stringsAsFactors = TRUE)
    
  }
  
  if(vuelta > 1){
    data_temp = fread(file, 
                      header = TRUE, 
                      sep = "\t", 
                      encoding = 'UTF-8',
                      #select = columnas_importar,
                      stringsAsFactors = TRUE)
    data_merge = smartbind(data_merge, data_temp)
  }
  
  vuelta = vuelta +1
}
Sys.time()

rm("data_temp")

# Por ahora no corto los dias... luego veremos si la base crece demasiado
# data_merge = data_merge[(data_merge$Fecha >= fecha_corte_merge),]


file_out = "Nomina_CAT_Acumulado.txt"
salida = paste(directorio_out, file_out, sep = "")

fwrite(data_merge, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")



# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Merge_NominaDiaria_GeneraAcumulado"

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

