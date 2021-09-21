#------------------------------------------------------------------------------------------
# Análisis CECAITRA

# Se realizo un ashx para que puedan obtener los registros en un csv en "crudo"
# http://10.68.11.51:8001/Ops/Indicadores.ashx?m=202009&f=ausa
# Los parámetros son:
# m: año y mes de captura de las presunciones
# f: flujo (cecaitra, cordoba, ausa, dvial, anpr)

# Los datos obtenidos son:
# - Flujo
# - Dispositivo (nombre de la cámara)
# - Captura (fecha de captura de la presunción)
# - Registro (fecha de importación al sistema)
# - Proceso (fecha de procesamiento del CPI)
# - Decisión
# - Calibración (se indica si la calibración está vencida)
# - Envío a DGAI (fecha de loteo y envío a DGAI)

#------------------------------------------------------------------------------------------

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)
library(googlesheets4)

#------------------ DEFINIR LOS MESES QUE SE TRABAJARAN------------------------------------
cant_meses = 3

diccionario_meses = data.frame(matrix(ncol = 3, nrow = 12))
diccionario_meses$X1 = c(1,2,3,4,5,6,7,8,9,10,11,12)
diccionario_meses$X2 = c("Enero", "Febrero", "Marzo", "Abril", "Mayo",
                         "Junio", "Julio", "Agosto", "Septiembre",
                         "Octubre", "Noviembre", "Diciembre")
diccionario_meses$X3 = c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
colnames(diccionario_meses) = c("Mes_Nro", "Mes_Nombre", "Mes_Txt")

fecha_en_curso = today()
#fecha_en_curso = as.Date("2016-12-31")

mes_en_curso = month(fecha_en_curso)

meses = as.list(matrix(ncol = cant_meses, nrow = 1))
fechas_meses = matrix(nrow = cant_meses, ncol = 5)

for (i in (1:cant_meses)){
  j = month(fecha_en_curso %m-% months(i-1))
  anio = year(fecha_en_curso %m-% months(i-1))

  meses[i] = diccionario_meses[which(diccionario_meses$Mes_Nro == j),2]
  fechas_meses[i,1] = anio
  fechas_meses[i,2] = j
  fechas_meses[i,3] = diccionario_meses[which(diccionario_meses$Mes_Nro == j),3]
  fechas_meses[i,4] = diccionario_meses[which(diccionario_meses$Mes_Nro == j),2]
  fechas_meses[i,5] = paste(anio, diccionario_meses[which(diccionario_meses$Mes_Nro == j),3], sep = "")
}

fechas_meses = as.data.frame(fechas_meses)
colnames(fechas_meses) = c("Año", "Mes", "Mes_00", "Mes_TXT", "AñoMes")
 
fecha_corte = make_date(anio, j, 1)

#---------------------------------------------------------------------------------------------

# Descargo los archivos
flujo = c("cecaitra", "cordoba", "ausa", "dvial", "anpr", "cascos")
añomes = fechas_meses$AñoMes

nombres = c("Fuente", "Dispositivo", "Fecha_Presunción", "Fecha_Import_a_Sistema", "Fecha_Proc_CPI", 
            "Decision", "Calibración", "Fecha_DGAI", "Agente")

nombres_cantidad = c("Fuente", "Dispositivo", "Fecha_Presunción", "Fecha_Import_a_Sistema", "Fecha_Proc_CPI", 
                     "Decision", "Calibración", "Fecha_DGAI", "Agente", "Cantidad")

data_ = data.frame(matrix(ncol = 10, nrow = 0))

colnames(data_) = nombres_cantidad

clases = c("character", "character", "IDate", "IDate", "IDate", "character", "character", "IDate", "character")

# Donde Guardo los Archivos
directorio_out = "W:\\Agentes Viales\\CPI\\Bajadas\\Fracciones\\"
directorio_out_temp= "W:\\Agentes Viales\\CPI\\Bajadas\\Fracciones\\Temp\\"

# item = "cecaitra"
# fecha = añomes[3]

for (item in flujo){
 for (fecha in añomes){
   url_ = paste("http://10.68.11.51:8001/Ops/Indicadores.ashx?m=", fecha, "&f=", item, sep = "")
   tryCatch({
     nombre_salida = paste(item, "_", fecha, ".txt", sep = "" )
     nombre_salida_temp = paste(item, "_", fecha, "_temp.txt", sep = "" )
     salida = paste(directorio_out, nombre_salida, sep = "")
     salida_temp = paste(directorio_out_temp, nombre_salida_temp, sep = "")
     salida_temp_2 = paste(directorio_out_temp, nombre_salida, sep = "")
     
     download.file(url = url_,
                   destfile = salida_temp,
                   method = 'curl')
     
     if (file.info(salida_temp)$size > file.info(salida)$size  | is.na(file.info(salida)$size)){
       file.rename(salida_temp, salida_temp_2)
       file.copy(salida_temp_2, salida, overwrite = TRUE)
     } 
         
     data = fread(salida, 
                  header = FALSE, 
                  sep = ",", 
                  encoding = 'UTF-8',
                  col.names = nombres,
                  colClasses = clases,
                  stringsAsFactors = TRUE)
     
     data = data %>%
       group_by(Fuente, Dispositivo, Fecha_Presunción, Fecha_Import_a_Sistema, Fecha_Proc_CPI, 
                Decision, Calibración, Fecha_DGAI, Agente) %>%
       summarise(count=n())
     
     colnames(data) = nombres_cantidad
     
     data_ = rbind(data, data_)
     
   }, error=function(e){})
 }
}

file.remove(list.files(directorio_out_temp, full.names = TRUE))

data = data_

data = data %>%
  group_by(Fuente, Dispositivo, Fecha_Presunción, Fecha_Import_a_Sistema, Fecha_Proc_CPI, 
           Decision, Calibración, Fecha_DGAI, Agente) %>%
  summarise(Cantidad_ = sum(Cantidad))

colnames(data) = nombres_cantidad

data$Fecha_Presunción = as.IDate(data$Fecha_Presunción)
data$Fecha_Import_a_Sistema = as.IDate(data$Fecha_Import_a_Sistema)
data$Fecha_Proc_CPI = as.IDate(data$Fecha_Proc_CPI)
data$Fecha_DGAI = as.IDate(data$Fecha_DGAI)

# Abro el histórico
directorio = "W:\\Agentes Viales\\CPI\\Bajadas\\"
setwd(directorio)
file = "Historico.txt"
file = paste(directorio, file, sep = "")
data_historico = fread(file, header = TRUE, sep = "\t", encoding = 'UTF-8')
data_historico = data_historico[,1:10]
colnames(data_historico) = nombres_cantidad

data_historico = data_historico[which(data_historico$Fecha_Import_a_Sistema < fecha_corte),]

data = rbind(data_historico, data,  fill=TRUE)

decisiones = as.data.frame(unique(data$Decision))
colnames(decisiones) = c("Decision")

# Guardo el archivo
directorio = "W:\\Agentes Viales\\CPI\\Bajadas\\"
setwd(directorio)
file = "Historico.txt"
file_decisiones = "Ref_Decisiones.txt"

nombre_salida = file
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")
fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

nombre_salida_ref = file_decisiones
directorio_out = directorio
salida = paste(directorio_out, nombre_salida_ref, sep = "")
fwrite(decisiones, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# Guardo el archivo con agregado mensual

data_mensual = data

data_mensual$Fecha_Presunción = as.IDate(ISOdate(year(data_mensual$Fecha_Presunción),
                                                 month(data_mensual$Fecha_Presunción),
                                                 1))
data_mensual$Fecha_Import_a_Sistema = as.IDate(ISOdate(year(data_mensual$Fecha_Import_a_Sistema),
                                                       month(data_mensual$Fecha_Import_a_Sistema),
                                                       1))
data_mensual$Fecha_Proc_CPI = as.IDate(ISOdate(year(data_mensual$Fecha_Proc_CPI),
                                               month(data_mensual$Fecha_Proc_CPI),
                                               1))

data_mensual$Fecha_DGAI = as.IDate(ISOdate(year(data_mensual$Fecha_DGAI),
                                               month(data_mensual$Fecha_DGAI),
                                               1))

data_mensual = data_mensual %>% 
  group_by(Fuente, Dispositivo, Fecha_Presunción, Fecha_Import_a_Sistema, Fecha_Proc_CPI, 
           Decision, Calibración, Fecha_DGAI, Agente) %>%
  summarise(Cantidad_ = sum(Cantidad))

colnames(data) = nombres_cantidad

directorio = "W:\\Agentes Viales\\CPI\\Bajadas\\"
setwd(directorio)
file = "Historico_Mensual.txt"

nombre_salida = file
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")
fwrite(data_mensual, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# Guardo la lista de agentes

agentes = as.data.frame(unique(data_mensual$Agente))
colnames(agentes) = c("Nombre_Agente")

file = "Agentes.txt"

directorio = "W:\\Agentes Viales\\CPI\\Ref\\"
setwd(directorio)

nombre_salida = file
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")
fwrite(agentes, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Descarga_Bases_CPI"

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
