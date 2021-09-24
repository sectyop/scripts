# Limpio la memoria
rm(list=ls())
gc()


# Cargo librerías
library(tidyverse)
library(lubridate)
library(tidyr)
library(data.table)
library(googlesheets4)

directorio_out = "W:\\API_Transporte\\Lecturas_LPR\\"
setwd(directorio_out)


# Obtengo las fechas del último mes
fecha_max = as.Date(today() - 1)
fecha_min = as.Date(make_date(year = year(today()),
                      month = month(today())-4,
                      day = 1))

# Voy a descargar únicamente la última semana
fecha_corte = fecha_min - 1
#fecha_corte = as.Date(make_date(2020,10,18))

dias = as.numeric(fecha_max - fecha_corte)

fechas = c()
flag = 0
#data = NULL
a = Sys.time()
# Descargo el .zip fecha por fecha (últ 7d), descomprimo y voy armando un único dataset
for (i in (1:dias)){
  fecha = fecha_corte + i
  print(fecha)

  nombre_archivo = paste(as.character(fecha), ".zip", sep = "")
  nombre_csv = paste("Detections_", as.character(fecha), ".csv", sep = "")
  data_temp = read.table(unz(nombre_archivo, nombre_csv), 
                  sep = ",",
                  header = TRUE)
  
  if (nrow(data_temp) > 0){
    data_temp$latitude = as.numeric(data_temp$latitude)
    data_temp$longitude = as.numeric(data_temp$longitude)
    
    data_temp$timestamp = as_datetime(data_temp$timestamp)
    data_temp$fecha = as_date(data_temp$timestamp)
    data_temp$hora = hour(data_temp$timestamp)
    
    data_temp$Año = year(data_temp$fecha)
    data_temp$Mes = month(data_temp$fecha)
    
    data_temp$patente_aux1 = as.integer(substr(data_temp$plate,3,5)) + 0
    data_temp$patente_aux2 = as.integer(substr(data_temp$plate,nchar(data_temp$plate)-2, nchar(data_temp$plate)))+0
    
    data_temp$tipo_patente = ifelse(is.na(data_temp$patente_aux1) & is.na(data_temp$patente_aux2),
                                    "Moto",
                                    "Auto")
    
    data_temp = data_temp %>%
      group_by(Año, Mes, fecha, hora, device_code, latitude, longitude, tipo_patente) %>%
      summarise(Cantidad = n())
    
  }else{
    next
  }
  
  if(flag == 0){
    data = data_temp
    flag = 1
  }else{
    data = rbind(data, data_temp)
  }
}
b = Sys.time()


#rm("data_temp")  

# Abro el historico
directorio_out = "W:\\API_Transporte\\"
setwd(directorio_out)

nombre_salida = "Lecturas_LPR_completo.txt"
salida = paste(directorio_out, nombre_salida, sep = "")

data_historico = fread(salida)

data_historico = data_historico[which(data_historico$fecha < fecha_corte), ]


if(nrow(data_temp)>0){
  data$fecha = as.IDate(data$fecha)
  
  data_historico = rbind(data_historico, data)
}


# Guardo el resultado

fwrite(data_historico, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")



# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Descarga_Lecturas_LPR_sintope"

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

