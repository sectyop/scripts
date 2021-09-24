# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(readxl)
library(googledrive)
library(googlesheets4)
library(dplyr)

directorio = "W:\\Agentes Viales\\No Programados\\"
setwd(directorio)
drive_auth(email = "datos.ssgm@gmail.com")

# Descargamos la base desde el Google Drive
#id_snp = drive_get("Registro Diario CGM - CUCC")$id
id_snp = "1mxzFn0PjGxVGTgmtrvDa7LIUFoynQ6_1f4sHbx102Mw"
archivo = as_id(id_snp)
nombre_archivo = "Registro Diario CGM - CUCC.xlsx"

# Descargo el archivo
directorio = "W:\\Agentes Viales\\No Programados\\"
path_ = paste(directorio, nombre_archivo, sep = "")

drive_download(
  file = archivo,
  path = path_,
  overwrite = TRUE)

# Leo el archivo

clases = c("numeric", "date", "date", "text", "text",
           "text", "text", "text", "text", "text", "text", "text",
           "text", "text", "text", "text", "date",
           "text", "text", "text", "date", "date",
           "text", "text", "guess", "guess", "guess",
           "guess", "guess", "guess", "guess", "guess")

data_temp = read_xlsx(path = path_,
                      .name_repair = "unique",
                      sheet = "Datos Operativos",
                      col_types = clases,
                      skip = 3)

data_temp = data_temp[which(!is.na(data_temp$`Fecha Nec`)),(1:22)]

# hora_retiro = as.data.frame(data_temp$`Hora Retiro Ag Pto`)
# colnames(hora_retiro) = c("Hora_Retiro")
# hora_retiro_lleno = as.data.frame(hora_retiro[!is.na(hora_retiro$Hora_Retiro),])
# colnames(hora_retiro_lleno) = c("Hora_Retiro")

# Corrijo todos los campos de fecha y hora
data_temp$`Fecha Nec` = as.IDate(data_temp$`Fecha Nec`)

data_temp$`Hora Recep` = hour(data_temp$`Hora Recep`)/24 + minute(data_temp$`Hora Recep`)/(24*60)
data_temp$`Hora Orden Conf` = hour(data_temp$`Hora Orden Conf`)/24 + minute(data_temp$`Hora Orden Conf`)/(24*60)
data_temp$`Hora Llegada` = hour(data_temp$`Hora Llegada`)/24 + minute(data_temp$`Hora Llegada`)/(24*60)
data_temp$`Hora Retiro Ag Pto` = hour(data_temp$`Hora Retiro Ag Pto`)/24 + minute(data_temp$`Hora Retiro Ag Pto`)/(24*60)


#hora_retiro_lleno$Hora_Retiro = hour(hora_retiro_lleno$Hora_Retiro)/24 + minute(hora_retiro_lleno$Hora_Retiro)/(24*60)

# Corrijo el tipo de dato del número de suceso
data_temp$`Nº Inc` = as.integer(data_temp$`Nº Inc`)

# Abro el histórico
file = "Historico_SNP.txt"
directorio = "W:\\Agentes Viales\\No Programados\\"
archivo = paste(directorio, file, sep = "")

nombres = colnames(data_temp)
clases = c("numeric", "IDate", "IDate", "character", "character",
           "character", "character", "character", "character", "character", "character", "character",
           "character", "character", "character", "character", "IDate",
           "character", "character", "character", "IDate", "IDate")

data_historico = fread(archivo,
                       header = TRUE, 
                       sep = "\t", 
                       encoding = 'UTF-8',
                       colClasses = clases,
                       col.names = nombres)

# Uno ambos archivos
# data_temp$`Fecha Nec` = as.IDate(data_temp$`Fecha Nec`)
# data_temp$`Hora Recep` = as.IDate(data_temp$`Hora Recep`)
data = rbind(data_historico, data_temp)

# Elimino duplicados
data = unique(data)


# -----------------------------------------------------------------------
# 2021/05/20 : Agrego info de la planilla de semáforos
# -----------------------------------------------------------------------
file_out = paste("Backup_Semaforos_CGM.txt", sep="")
directorio_out = "W:\\CGM\\Backup_Semaforos\\"
setwd(directorio_out)
salida = paste(directorio_out, file_out, sep = "")

historico_semaforos = fread(salida,
                            encoding = 'UTF-8',
                            sep = "\t")

historico_semaforos = historico_semaforos[which(tolower(historico_semaforos$ESTADO) != "cancelado"), ]

#nombres_columnas = c('CALLE 1 (Principal)', 'Calle 2 (Intersección)', 'Sub-tipo', 'ESTADO TÉCNICOS', 'INPUT', 'Comentarios CGM', 
#                    'ESTADO', 'Envía Agente', 'Descripción de Suceso', 'Medio Comunicación', 'JUSTIFICATIVO (CUANDO NO ENVÍAN)', 
#                     'Base Asignada', 'Observaciones (detalle del suceso)', 'Criticidad', 'BASE CAT CORRESP.', 'CALLE 3', 
#                     'INICIO SUCESO', 'UPS', 'TITILANTE', 'ZONAS SL', 'ID', 'CÁMARA SGIM', 'CÁMARA POLICÍA', 'CÁMARAS LÍNEAS', 
#                     'COMUNA', 'PUNTO FIJO POLICIA/PNA/GNA', 'Hora Orden Conf', 'Hora Llegada', 'Hora Retiro Ag Pto', 'FIN SUCESO')

fecha_cambio = as.Date("2021-07-01")
if (today() >= fecha_cambio){
  nombres_columnas = c('CALLE 1 (Principal)', 'Calle 2 (Intersección)', 'Sub-tipo', 'ESTADO TÉCNICOS', '¿Quién lo pide?', 
                       'OBSERVACIONES CGM', 'ESTADO', 'Envía Agente', 'Descripción de Suceso', 'Medio Comunicación', 
                       'Justif \nCuando no envian', 'Base Asignada', 'Observaciones (detalle del suceso)', 'Criticidad', 
                       'BASE CAT CORRESP.', 'CALLE 3', 'INICIO SUCESO', 'UPS', 'TITILANTE', 'ZONAS SL', 'ID', 'CÁMARA SGIM', 
                       'CÁMARA POLICÍA', 'CÁMARAS LÍNEAS', 'COMUNA', 'PUNTO FIJO POLICIA/PNA/GNA', 'Hora Orden Conf', 
                       'Hora Llegada', 'Hora Retiro Ag Pto', 'FIN SUCESO', 'ESTADO AGENTES_guardar', 'ESTADO SUCESO')
  
  columnas_eliminar = c('ESTADO TÉCNICOS', 'OBSERVACIONES CGM', 'ESTADO', 'BASE CAT CORRESP.', 'CALLE 3', 'UPS', 'TITILANTE', 
                        'ZONAS SL', 'ID', 'CÁMARA SGIM', 'CÁMARA POLICÍA', 'CÁMARAS LÍNEAS', 'COMUNA', 'PUNTO FIJO POLICIA/PNA/GNA', 'FIN SUCESO',
                        'INICIO SUCESO', 'ESTADO SUCESO')
}else{
  nombres_columnas = c('CALLE 1 (Principal)', 'Calle 2 (Intersección)', 'Sub-tipo', 'ESTADO TÉCNICOS', '¿Quién lo pide?', 
                       'OBSERVACIONES CGM', 'ESTADO', 'Envía Agente', 'Descripción de Suceso', 'Medio Comunicación', 
                       'Justif \nCuando no envian', 'Base Asignada', 'Observaciones (detalle del suceso)', 'Criticidad', 
                       'BASE CAT CORRESP.', 'CALLE 3', 'INICIO SUCESO', 'UPS', 'TITILANTE', 'ZONAS SL', 'ID', 'CÁMARA SGIM', 
                       'CÁMARA POLICÍA', 'CÁMARAS LÍNEAS', 'COMUNA', 'PUNTO FIJO POLICIA/PNA/GNA', 'Hora Orden Conf', 
                       'Hora Llegada', 'Hora Retiro Ag Pto', 'FIN SUCESO')
  
  columnas_eliminar = c('ESTADO TÉCNICOS', 'OBSERVACIONES CGM', 'ESTADO', 'BASE CAT CORRESP.', 'CALLE 3', 'UPS', 'TITILANTE', 
                        'ZONAS SL', 'ID', 'CÁMARA SGIM', 'CÁMARA POLICÍA', 'CÁMARAS LÍNEAS', 'COMUNA', 'PUNTO FIJO POLICIA/PNA/GNA', 'FIN SUCESO',
                        'INICIO SUCESO')
}


# Homogeinizo nombres de columnas entre los dos datasets (las que coinciden al menos)
colnames(historico_semaforos) = nombres_columnas

# Voy a utilizar ESTADO AGENTES en lugar de Envia Agentes
# historico_semaforos$`Envía Agente` = historico_semaforos$`ESTADO AGENTES`
# 27/07/2021 a pedido de MA, dejo esta columna tal como está

# Agrego las que están en SNP y no en semaforos
historico_semaforos$`Tipo Op` = "Semáforos"
historico_semaforos$`Tipo Obstruccion` = "Ninguna"
historico_semaforos$`P/NP` = "NP"

historico_semaforos$`Fecha Nec` = as.IDate(historico_semaforos$`INICIO SUCESO`)
historico_semaforos$`Hora Recep` = as.character(hour(historico_semaforos$`INICIO SUCESO`)/24 + 
                                                  minute(historico_semaforos$`INICIO SUCESO`)/(24*60))
  
filas = nrow(historico_semaforos)
columnas_agregar = data.frame(matrix(ncol = 3, nrow = filas))
colnames(columnas_agregar) = c('Nº Inc', 'URL Google Maps', 'LLego Ag')

historico_semaforos = cbind(historico_semaforos, columnas_agregar)

# Elimino las columnas de semaforos que no están en la base de SNP
historico_semaforos[, columnas_eliminar] = list(NULL)

# Ordeno las columnas de ambos de igual manera
# colnames(data)
# colnames(historico_semaforos)
# Agrego la columna 'ESTADO AGENTES_guardar' a Base SNP
filas = nrow(data)
columnas_agregar = data.frame(matrix(ncol = 1, nrow = filas))
colnames(columnas_agregar) = c('ESTADO AGENTES_guardar')
data = cbind(data, columnas_agregar)

orden = colnames(data)
historico_semaforos = historico_semaforos %>% select(all_of(orden))
colnames(historico_semaforos) = orden

historico_semaforos$`Hora Orden Conf` = ymd_hms(historico_semaforos$`Hora Orden Conf`)
historico_semaforos$`Hora Llegada` = ymd_hms(historico_semaforos$`Hora Llegada`)
historico_semaforos$`Hora Retiro Ag Pto` = ymd_hms(historico_semaforos$`Hora Retiro Ag Pto`)

historico_semaforos$`Hora Orden Conf` = hour(historico_semaforos$`Hora Orden Conf`)/24 + minute(historico_semaforos$`Hora Orden Conf`)/(24*60)
historico_semaforos$`Hora Llegada` = hour(historico_semaforos$`Hora Llegada`)/24 + minute(historico_semaforos$`Hora Llegada`)/(24*60)
historico_semaforos$`Hora Retiro Ag Pto` = hour(historico_semaforos$`Hora Retiro Ag Pto`)/24 + minute(historico_semaforos$`Hora Retiro Ag Pto`)/(24*60)


# Veo la fecha minima en que empecé a usar la nueva planilla de semáforos
fecha_aux = as.data.frame(ifelse(historico_semaforos$`Fecha Nec` > as.IDate("1899-12-31"),
                   as.character(historico_semaforos$`Fecha Nec`),
                   NA))
colnames(fecha_aux) = c("Aux")
fecha_aux = fecha_aux %>% distinct(Aux)
fecha_corte = min(fecha_aux$Aux, na.rm = TRUE)

data$aux1 = tolower(data$`Tipo Op`)
data$aux2 = substr(data$aux1,1,2)

data$aux3 = ifelse(data$aux2 == "se" & data$`Fecha Nec` > fecha_corte,
                   1,
                   0)

data_sin_semaforos_nuevos = data[which(data$aux3 == 0),]
data_sin_semaforos_nuevos$aux1 = NULL
data_sin_semaforos_nuevos$aux2 = NULL
data_sin_semaforos_nuevos$aux3 = NULL

data_final = rbind(historico_semaforos, data_sin_semaforos_nuevos)

nombres_columnas = c('N Inc', 'Fecha Nec', 'Hora Recep', 'Tipo Op', 'Sub-tipo', 
                     'Tipo Obstruccion', 'P/NP', 'CALLE 1 (Principal)', 'Calle 2 (Interseccion)', 'URL Google Maps',
                     'Descripcion de Suceso', 'Quien lo pide', 'Medio Comunicacion', 'Criticidad', 'Envia Agente',
                     'Justif Cuando no envian', 'Hora Orden Conf', 'Base Asignada', 'LLego Ag', 'Observaciones (detalle del suceso)',
                     'Hora Llegada', 'Hora Retiro Ag Pto', 'ESTADO AGENTES_guardar')

colnames(data_final) = nombres_columnas

# Guardo el archivo de trabajo
file_out = "Datos_SNP.txt"
directorio_out = directorio
setwd(directorio_out)

salida = paste(directorio_out, file_out, sep = "")

#fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
fwrite(data_final, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "SNP_Arma_Datos"

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


