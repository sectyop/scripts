# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)
library(bit64)
#library(plyr)

#------------------ DEFINIR LOS MESES QUE SE TRABAJARÁN---------------------------------

if(day(today()) < 15) {
  cant_meses = 2
}else{
  cant_meses = 1
}

#cant_meses = 2

diccionario_meses = data.frame(matrix(ncol = 3, nrow = 12))
diccionario_meses$X1 = c(1,2,3,4,5,6,7,8,9,10,11,12)
diccionario_meses$X2 = c("Enero", "Febrero", "Marzo", "Abril", "Mayo",
                         "Junio", "Julio", "Agosto", "Septiembre",
                         "Octubre", "Noviembre", "Diciembre")
diccionario_meses$X3 = c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
colnames(diccionario_meses) = c("Mes_Nro", "Mes_Nombre", "Mes_Txt")

fecha_en_curso = today()
#fecha_en_curso = as.Date("2020-07-31")
mes_en_curso = month(fecha_en_curso)

meses = as.list(matrix(ncol = cant_meses, nrow = 1))
fechas_meses = matrix(nrow = cant_meses, ncol = 4)

for (i in (1:cant_meses)){
  j = month(fecha_en_curso %m-% months(i-1))
  anio = year(fecha_en_curso %m-% months(i-1))
  
  meses[i] = diccionario_meses[which(diccionario_meses$Mes_Nro == j),2]
  fechas_meses[i,1] = anio
  fechas_meses[i,2] = j
  fechas_meses[i,3] = diccionario_meses[which(diccionario_meses$Mes_Nro == j),3]
  fechas_meses[i,4] = diccionario_meses[which(diccionario_meses$Mes_Nro == j),2]
}

fechas_meses = as.data.frame(fechas_meses)
fechas_meses$Fecha_Corte = ISOdate(fechas_meses$V1, fechas_meses$V2,1, hour = 0)
fechas_meses$nombre_salida = paste("SUBE_Horario_", year(fechas_meses$Fecha_Corte), fechas_meses$V3, ".txt", sep = "")
fechas_meses$Anio_Corte = year(fechas_meses$Fecha_Corte)
fechas_meses$Mes_Corte = month(fechas_meses$Fecha_Corte)

directorio = "W:\\SUBE\\Transacciones\\"
fechas_meses$Directorio = paste(directorio, year(fechas_meses$Fecha_Corte), "\\", fechas_meses$V4, "\\", sep = "")

fechas_meses$V1 = NULL
fechas_meses$V2 = NULL
fechas_meses$V3 = NULL
fechas_meses$V4 = NULL

fecha_corte = make_date(anio, j, 1) 
fecha_corte_merge = as.Date("2020-03-01")
#---------------------------------------------------------------------------------------------

directorio = "W:\\SUBE\\Transacciones\\"
setwd(directorio)

#---------------------------------------------------------------------------------------------
# Poner esta variable en 1 si queremos ejecutar aunque no sea domingo... tmb ver el if si se quiere cambiar el dia
# forzar = 1
forzar = 0

# if(weekdays(today()) == "domingo"){
#   forzar = 1
# }

if(weekdays(today()) == "lunes"){
  forzar = 1
}
#---------------------------------------------------------------------------------------------


archivo_ref = "W:\\SUBE\\Ref_Lineas_NLS.csv"
ref_lineas = fread(archivo_ref, header = TRUE, sep = ";", encoding = 'UTF-8')

archivo_ref_contratos = "W:\\SUBE\\Ref_Contratos.csv"
ref_contratos = fread(archivo_ref_contratos, header = TRUE, sep = ";", encoding = 'UTF-8')



directorios = fechas_meses$Directorio
i = 0

if(forzar == 1) {
  columnas_importar = c("IDLINEA", "INTERNO", "FECHATRX", "CODIGOCONTRATO", 
                        "CODIGOROL", "VALOR_TARIFA", "MONTO")
  data_mes = data.frame(matrix(ncol = 9, nrow = 0))
}else{
  columnas_importar = c("IDLINEA", "INTERNO", "FECHATRX")
  data_mes = data.frame(matrix(ncol = 5, nrow = 0))
}

# Área de prueba --------------------------------------------------

# file = "W:\\SUBE\\Transacciones\\2021\\Febrero\\CABA_Transacciones_19022021.csv.gz"
# data = fread(file, 
#              header = TRUE, 
#              sep = ";", 
#              encoding = 'UTF-8',
#              select = columnas_importar,
#              dec = ",",
#              stringsAsFactors = TRUE)
# 
# data$Fecha = dmy_hms(data$FECHATRX)
# 
# data$Fecha_ = date(data$Fecha)
# data$Hora_ = hour(data$Fecha)
# data$Tarifa_Debitada = data$VALOR_TARIFA - data$MONTO
# 
# #columnas_a_eliminar = c("FECHATRX", "Fecha", "VALOR_TARIFA", "MONTO")
# columnas_a_eliminar = c("FECHATRX", "Fecha", "VALOR_TARIFA")
# data[, columnas_a_eliminar] = list(NULL)
# 
# data = data %>%
#   group_by(Fecha_, Hora_, IDLINEA, INTERNO, CODIGOCONTRATO, CODIGOROL, MONTO, Tarifa_Debitada) %>%
#   summarise(count=n())
# 
# contratos = unique(data$CODIGOCONTRATO)

# -------------------------------------------------------------------

for (directorio_mes in directorios){
#for (mes in meses){
  # directorio_mes = directorios[1]
  # file = "CABA_Transacciones_19042021.csv.gz"
  
  i = i + 1
  setwd(directorio_mes)
  
  Allfiles = list.files()
  vuelta = 1
  
  for (file in Allfiles){
    if(vuelta < 2){
      print(paste(vuelta, file))
      data = fread(file, 
                   header = TRUE, 
                   sep = ";", 
                   encoding = 'UTF-8',
                   select = columnas_importar,
                   dec = ",",
                   stringsAsFactors = TRUE)
      
      data$Fecha = dmy_hms(data$FECHATRX)
      
      data$Fecha_ = date(data$Fecha)
      data$Hora_ = hour(data$Fecha)
      
      if(forzar == 1) {
        data$Tarifa_Debitada = data$VALOR_TARIFA - data$MONTO
      }

      #columnas_a_eliminar = c("FECHATRX", "Fecha", "VALOR_TARIFA", "MONTO")
      columnas_a_eliminar = c("FECHATRX", "Fecha", "VALOR_TARIFA")
      data[, columnas_a_eliminar] = list(NULL)
      
      if(forzar == 1) {
        data = data %>%
          group_by(Fecha_, Hora_, IDLINEA, INTERNO, CODIGOCONTRATO, CODIGOROL, MONTO, Tarifa_Debitada) %>%
          summarise(count=n())
      }else{
        data = data %>%
          group_by(Fecha_, Hora_, IDLINEA, INTERNO) %>%
          summarise(count=n())
      }
    }
    if(vuelta > 1){
      print(paste(vuelta, file))
      data_temp = fread(file, 
                        header = TRUE, 
                        sep = ";", 
                        encoding = 'UTF-8',
                        select = columnas_importar,
                        dec = ",",
                        stringsAsFactors = TRUE)
      

      data_temp$Fecha = dmy_hms(data_temp$FECHATRX)
      
      data_temp$Fecha_ = date(data_temp$Fecha)
      data_temp$Hora_ = hour(data_temp$Fecha)
      
      if(forzar == 1) {
        data_temp$Tarifa_Debitada = data_temp$VALOR_TARIFA - data_temp$MONTO
      }

      #columnas_a_eliminar = c("FECHATRX", "Fecha", "VALOR_TARIFA", "MONTO")
      columnas_a_eliminar = c("FECHATRX", "Fecha", "VALOR_TARIFA")
      data_temp[, columnas_a_eliminar] = list(NULL)
      
      if(forzar == 1) {
        data_temp = data_temp %>%
          group_by(Fecha_, Hora_, IDLINEA, INTERNO, CODIGOCONTRATO, CODIGOROL, MONTO, Tarifa_Debitada) %>%
          summarise(count=n())
      }else{
        data_temp = data_temp %>%
          group_by(Fecha_, Hora_, IDLINEA, INTERNO) %>%
          summarise(count=n())
      }
      
      data$INTERNO = as.integer(data$INTERNO)
      data_temp$INTERNO = as.integer(data_temp$INTERNO)
      
      data = rbind(data, data_temp)
    }
    
    vuelta = vuelta +1
  }

  nombres = colnames(data)
  colnames(data_mes) = nombres
  
  data_bck = data
  
  
  # ----------------------------------------------------------------------------------------
  # Acá guardo los archivos con toda la info del mes (según el directorio en que esté...)
  
  if(forzar == 1) {
    colnames(data) = c("Fecha", "Hora", "Id_Linea", "Interno", "Contrato","Codigo_Rol", "Monto", 
                       "Tarifa_Debitada", "Cantidad")
  }else{
    colnames(data) = c("Fecha", "Hora", "Id_Linea", "Interno", "Cantidad")
  }

  data = merge(data, ref_lineas, by.x = "Id_Linea", by.y = "ID_LINEA", all.x = TRUE)
  
  #data = merge(data, ref_contratos, by.x = "Contrato", by.y = "CODIGOCONTRATO", all.x = TRUE)
  
  data = data %>%
    select(Fecha, Hora, Medio, NOMBRE_EMPRESA, Linea_Corregido, Cantidad)
  
  colnames(data) = c("Fecha", "Hora", "Medio", "Empresa", "Linea", "Cantidad")
  
  data = data %>%
    group_by(Fecha, Hora, Medio, Empresa, Linea) %>%
    summarise(Cantidad=sum(Cantidad))
  
  directorio_out = "W:\\SUBE\\Merge\\"
  setwd(directorio_out)
  
  #data_ =  data[which( (year(data$Fecha) == fechas_meses[i,3]) & (month(data$Fecha) == fechas_meses[i,4]) ),]
  nombre_salida = fechas_meses[i,2]
  salida = paste(directorio_out, nombre_salida, sep = "")
  fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  
  # ----------------------------------------------------------------------------------------
  
  data_mes = rbind(data_mes, data_bck)
}

data = data_mes

if(forzar == 1) {
  colnames(data) = c("Fecha", "Hora", "Id_Linea", "Interno", "Contrato","Codigo_Rol", "Monto",
                     "Tarifa_Debitada", "Cantidad")
}else{
  colnames(data) = c("Fecha", "Hora", "Id_Linea", "Interno", "Cantidad")
}


rm("data_temp", "data_mes")

directorio_out = "W:\\SUBE\\"

lineas = data.frame(unique(data$Id_Linea))
nombre_lineas = "lineas_sube.txt"
salida_lineas = paste(directorio_out, nombre_lineas, sep = "")
fwrite(lineas, salida_lineas, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


data = merge(data, ref_lineas, by.x = "Id_Linea", by.y = "ID_LINEA", all.x = TRUE)


if(forzar == 1) {
  data = merge(data, ref_contratos, by.x = "Contrato", by.y = "CODIGOCONTRATO", all.x = TRUE)
  
  data = data %>%
    select(Fecha, Hora, Medio, NOMBRE_EMPRESA, Linea_Corregido, Interno, Contrato, Codigo_Rol, DESCRIPCION, 
           Monto, Tarifa_Debitada, Cantidad)
  
  colnames(data) = c("Fecha", "Hora", "Medio", "Empresa", "Linea", "Interno", 
                     "Contrato", "Codigo_Rol", "Descripcion_Contrato", "Monto",
                     "Tarifa_Debitada", "Cantidad")
}else{
  data = data %>%
    select(Fecha, Hora, Medio, NOMBRE_EMPRESA, Linea_Corregido, Interno, Cantidad)
  
  colnames(data) = c("Fecha", "Hora", "Medio", "Empresa", "Linea", "Interno", "Cantidad")
}

data = data[which(data$Fecha >= fecha_corte),]
data = data[which(data$Fecha <= fecha_en_curso),]



#----------------GUARDO EL ARCHIVO CON INTERNO Y CONTRATO------------------------

if(forzar == 1) {
  nombre_salida = "N:\\Colectivos\\SUBE_Horario_ConInterno_ConContrato.txt"
  #salida = paste(directorio_out, nombre_salida, sep = "")
  fwrite(data, nombre_salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  

  #----------------GUARDO EL ARCHIVO SIN INTERNO Y CON MONTO --- PEDIDO IB 20200205
  data2 = data
  data2 = data2 %>%
    group_by(Fecha, Hora, Medio, Empresa, Linea, Codigo_Rol, Contrato, Descripcion_Contrato, Monto) %>%
    summarise(Cantidad=sum(Cantidad))
  
  nombre_salida = "SUBE_Horario_SinInterno_ConMonto.txt"
  salida = paste(directorio_out, nombre_salida, sep = "")
  fwrite(data2, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  rm("data2")


  
}

#----------------GUARDO EL ARCHIVO CON INTERNO-----------------------------------
data = data %>%
  group_by(Fecha, Hora, Medio, Empresa, Linea, Interno) %>%
  summarise(Cantidad=sum(Cantidad))

nombre_salida = "SUBE_Horario_ConInterno.txt"
salida = paste(directorio_out, nombre_salida, sep = "")
fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")



#----------------GUARDO EL ARCHIVO-----------------------------------------------
# data = data %>%
#   group_by(Fecha, Hora, Medio, Empresa, Linea) %>%
#   summarise(Cantidad=sum(Cantidad))
# 
# directorio_out = "W:\\SUBE\\Merge\\"
# setwd(directorio_out)
# 
# 
# for (i in (1:cant_meses)){
#  data_ =  data[which( (year(data$Fecha) == fechas_meses[i,3]) & (month(data$Fecha) == fechas_meses[i,4]) ),]
#  nombre_salida = fechas_meses[i,2]
#  salida = paste(directorio_out, nombre_salida, sep = "")
#  fwrite(data_, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
# }
# 
# 
#inicio_mes_2 = make_date(year(today()), month(today()), 1)
#data_mes1 = data[which(data$Fecha < inicio_mes_2),]
#data_mes2 = data[which(data$Fecha >= inicio_mes_2),]
#data_lista = list(data_mes1, data_mes2)

#anio_corte = year(inicio_mes_2)

#directorio_out = "W:\\SUBE\\Merge\\"
#setwd(directorio_out)


#for (i in (1:2)){
#  data_ = data.frame(data_lista[i])
#  nombre_salida = paste("SUBE_Horario_", meses[i], anio_corte, ".txt", sep = "")
#  salida = paste(directorio_out, nombre_salida, sep = "")
#  fwrite(data_, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
#}

#fecha_dic = as.Date("2020-12-01")
#fecha_ene = as.Date("2021-01-01")

#data_dic = data[which(data$Fecha >= fecha_ene),]
#nombre_salida = "SUBE_Horario_Diciembre_2020.txt"
#salida = paste(directorio_out, nombre_salida, sep = "")
#fwrite(data_dic, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
#----------------GUARDO EL ARCHIVO-----------------------------------------------


# --------------------------------------- MERGE ARCHIVOS MENSUALES

directorio = "W:\\SUBE\\Merge"
directorio_out = "W:\\SUBE\\"
setwd(directorio)

Allfiles = list.files(pattern = "\\.txt$")


archivo_ref = "W:\\SUBE\\Ref_Lineas_NLS.csv"
ref_lineas = fread(archivo_ref, header = TRUE, sep = ";", encoding = 'UTF-8')

columnas_importar = c("Fecha", "Hora", "Medio", "Empresa", "Linea", "Cantidad")

vuelta = 1

Sys.time()
for (file in Allfiles){
  if(vuelta < 2){
    data_merge = fread(file, 
                 header = TRUE, 
                 sep = "\t", 
                 encoding = 'UTF-8',
                 select = columnas_importar,
                 stringsAsFactors = TRUE)

  }
  
  if(vuelta > 1){
    data_temp = fread(file, 
                      header = TRUE, 
                      sep = "\t", 
                      encoding = 'UTF-8',
                      select = columnas_importar,
                      stringsAsFactors = TRUE)
    data_merge = rbind(data_merge, data_temp)
  }
  
  vuelta = vuelta +1
}
Sys.time()

#warnings()

rm("data_temp")

data_merge = data_merge %>%
  group_by(Fecha, Hora, Medio, Empresa, Linea) %>%
  summarise(Cantidad_=sum(Cantidad, na.rm = TRUE))

colnames(data_merge) = c("Fecha", "Hora", "Medio", "Empresa", "Linea", "Cantidad")

#data$anio_ = year(data$Fecha)
#data$mes_ = month(data$Fecha)

#anio_corte_merge = 2020
#mes_corte_merge = 3


#data = data[data$anio_ == anio_corte_merge & data$mes_ >= mes_corte_merge,]
data_merge = data_merge[(data_merge$Fecha >= fecha_corte_merge) & (data_merge$Fecha < fecha_en_curso),]

nombre_salida = "SUBE_Horario.txt"
salida = paste(directorio_out, nombre_salida, sep = "")
fwrite(data_merge, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
library(googlesheets4)

gs4_auth(email = "nacho.ls@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Procesa_Transacciones_gz"

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

