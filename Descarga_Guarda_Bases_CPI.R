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
cant_meses = 12

diccionario_meses = data.frame(matrix(ncol = 3, nrow = 12))
diccionario_meses$X1 = c(1,2,3,4,5,6,7,8,9,10,11,12)
diccionario_meses$X2 = c("Enero", "Febrero", "Marzo", "Abril", "Mayo",
                         "Junio", "Julio", "Agosto", "Septiembre",
                         "Octubre", "Noviembre", "Diciembre")
diccionario_meses$X3 = c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
colnames(diccionario_meses) = c("Mes_Nro", "Mes_Nombre", "Mes_Txt")

fecha_en_curso = today()
# fecha_en_curso = as.Date("2020-05-31")

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
flujo = c("ausa", "cecaitra", "cordoba", "dvial", "anpr", "cascos")
#flujo = c("ausa")
#flujo = c("cecaitra")
añomes = fechas_meses$AñoMes
total = nrow(as.data.frame(flujo)) * nrow(as.data.frame(añomes))
vuelta = 0

data = data.frame(matrix(ncol = 9, nrow = 0))


# Donde Guardo los Archivos
directorio_out = "W:\\Agentes Viales\\CPI\\Bajadas\\Fracciones\\"
directorio_out_temp= "W:\\Agentes Viales\\CPI\\Bajadas\\Fracciones\\Temp_Historico\\"



# ------------------------------------------------------------------------ AREA DE PRUEBA DE CODIGO

# url_ = "http://10.68.11.51:8001/Ops/Indicadores.ashx?m=202006&f=cecaitra"
# 
# data = fread(url_)
# nombres = c("Fuente", "Dispositivo", "Fecha_Presunción", "Fecha_Import_a_Sistema", "Fecha_Proc_CPI", "Decision", "Calibración", "Fecha_DGAI")
# colnames(data) = nombres
# 
# data = data %>%
#   group_by(Fuente, Dispositivo, Fecha_Presunción, Fecha_Import_a_Sistema, Fecha_Proc_CPI, Decision, Calibración, Fecha_DGAI) %>%
#   summarise(count=n())
# 
# nombres = c("Fuente", "Dispositivo", "Fecha_Presunción", "Fecha_Import_a_Sistema", "Fecha_Proc_CPI",
#             "Decision", "Calibración", "Fecha_DGAI", "Cantidad")
# colnames(data) = nombres
# 
# #nombre_salida = paste(item, "_", fecha, ".txt", sep = "" )
# nombre_salida = "cecaitra_202006.txt"
# salida = paste(directorio_out, nombre_salida, sep = "")
# fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
# 
# download.file(url= url_,
#               destfile= salida, 
#               method='curl')

# ----------------------------------------------------------------------------------------------------

#item = flujo[1]
for (item in flujo){
  #directorio_out = paste("W:\\Agentes Viales\\CPI\\Bajadas\\Fracciones\\", item, "\\", sep = "")
  for (fecha in añomes){
    print(paste("Fuente: ", item, " // Periodo: ", fecha, sep = ""))
    url_ = paste("http://10.68.11.51:8001/Ops/Indicadores.ashx?m=", fecha, "&f=", item, sep = "")
    tryCatch({
      # data = fread(url_,
      #              header = FALSE,
      # )
      # 
      # #data = rbind(data, data_temp)
      # nombres = c("Fuente", "Dispositivo", "Fecha_Presunción", "Fecha_Import_a_Sistema", "Fecha_Proc_CPI", "Decision", "Calibración", "Fecha_DGAI")
      # colnames(data) = nombres
      # 
      # data = data %>%
      #   group_by(Fuente, Dispositivo, Fecha_Presunción, Fecha_Import_a_Sistema, Fecha_Proc_CPI, Decision, Calibración, Fecha_DGAI) %>%
      #   summarise(count=n())
      # 
      # nombres = c("Fuente", "Dispositivo", "Fecha_Presunción", "Fecha_Import_a_Sistema", "Fecha_Proc_CPI", 
      #             "Decision", "Calibración", "Fecha_DGAI", "Cantidad")
      # colnames(data) = nombres
      
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
      
      # fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
      
      
    }, error=function(e){})
  }
}




# Unifico los archivos y armo el histórico

directorio = "W:\\Agentes Viales\\CPI\\Bajadas\\Fracciones\\"
directorio_out = "W:\\Agentes Viales\\CPI\\Bajadas\\"

setwd(directorio)

Allfiles = list.files(pattern = "\\.txt$")

nombres = c("Fuente", "Dispositivo", "Fecha_Presunción", "Fecha_Import_a_Sistema", "Fecha_Proc_CPI", 
            "Decision", "Calibración", "Fecha_DGAI", "Agente")

nombres_cantidad = c("Fuente", "Dispositivo", "Fecha_Presunción", "Fecha_Import_a_Sistema", "Fecha_Proc_CPI", 
            "Decision", "Calibración", "Fecha_DGAI", "Agente", "Cantidad")

clases = c("character", "character", "IDate", "IDate", "IDate", "character", "character", "IDate", "character")

data = data.frame(matrix(ncol = 9, nrow = 0))

# file = "anpr_201902.txt"
# tamaño = file.info(file)$size
# data = fread(file,
#              header = FALSE,
#              sep = ",",
#              encoding = 'UTF-8',
#              col.names = nombres,
#              colClasses = clases,
#              stringsAsFactors = TRUE)

vuelta = 1

Sys.time()
for (file in Allfiles){
  print(paste("Archivo: ", file, " // Vuelta: ", vuelta, sep = ""))
  
  #file = "cascos_202009.txt"
  tamaño = file.info(file)$size
  if (tamaño > 500){
    
    if(vuelta < 2){
      
      data = fread(file, 
                   header = FALSE, 
                   sep = ",", 
                   encoding = 'UTF-8',
                   col.names = nombres,
                   colClasses = clases,
                   stringsAsFactors = TRUE)
      
      data = data %>%
        group_by(Fuente, Dispositivo, Fecha_Presunción, Fecha_Import_a_Sistema, Fecha_Proc_CPI, Decision, Calibración, Fecha_DGAI, Agente) %>%
        summarise(count=n())
      
      colnames(data) = nombres_cantidad
      
    }
    
    if(vuelta > 1){
      data_temp = fread(file, 
                        header = FALSE, 
                        sep = ",", 
                        encoding = 'UTF-8',
                        col.names = nombres,
                        colClasses = clases,
                        stringsAsFactors = TRUE)
      
      data_temp = data_temp %>%
        group_by(Fuente, Dispositivo, Fecha_Presunción, Fecha_Import_a_Sistema, Fecha_Proc_CPI, Decision, Calibración, Fecha_DGAI, Agente) %>%
        summarise(count=n())
      
      colnames(data_temp) = nombres_cantidad
      
      data = rbind(data, data_temp)
    }    
  }
  
  vuelta = vuelta +1
}
Sys.time()

#head(data)

data_bck = data

data = data %>%
  group_by(Fuente, Dispositivo, Fecha_Presunción, Fecha_Import_a_Sistema, Fecha_Proc_CPI, 
           Decision, Calibración, Fecha_DGAI, Agente) %>%
  summarise(Cantidad_ = sum(Cantidad))

colnames(data) = nombres_cantidad

nombre_salida = "Historico.txt"

salida = paste(directorio_out, nombre_salida, sep = "")
fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

file.remove(list.files(directorio_out_temp, full.names = TRUE))

# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Descarga_Guarda_Bases_CPI"

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

