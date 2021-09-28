# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)
library(bit64)

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
fechas_meses$nombre_salida = paste("SUBE_ColetivosEnCalle_", year(fechas_meses$Fecha_Corte), fechas_meses$V3, ".txt", sep = "")
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

columnas_importar = c("IDLINEA", "INTERNO", "DISPOSITIVO", "FECHATRX", "SAM_ID")

archivo_ref = "W:\\SUBE\\Ref_Lineas_NLS.csv"
ref_lineas = fread(archivo_ref, header = TRUE, sep = ";", encoding = 'UTF-8')

directorios = fechas_meses$Directorio
i = 0

#data_mes = data.frame(matrix(ncol = 7, nrow = 0))

# file = "W:\\SUBE\\Transacciones\\2020\\Noviembre\\CABA_Transacciones_09112020.csv.gz"
for (directorio_mes in directorios){

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
      
      columnas_a_eliminar = c("FECHATRX", "Fecha")
      data[, columnas_a_eliminar] = list(NULL)
      
      data$IDLINEA = as.integer(data$IDLINEA)
      
      data = data %>%
        group_by(Fecha_, Hora_, IDLINEA, INTERNO, DISPOSITIVO, SAM_ID) %>%
        summarise(count=n())

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
      
      columnas_a_eliminar = c("FECHATRX", "Fecha")
      data_temp[, columnas_a_eliminar] = list(NULL)
      
      data_temp$IDLINEA = as.integer(data_temp$IDLINEA)
      
      data_temp = data_temp %>%
        group_by(Fecha_, Hora_, IDLINEA, INTERNO, DISPOSITIVO, SAM_ID) %>%
        summarise(count=n())
      
      data$INTERNO = as.integer(data$INTERNO)
      data_temp$INTERNO = as.integer(data_temp$INTERNO)

      data = rbind(data, data_temp)
    }
    
    vuelta = vuelta +1
  }
  #warnings()
  
  #nombres = colnames(data)
  #colnames(data_mes) = nombres
  
  data_bck = data
  
  # ----------------------------------------------------------------------------------------
  # Acá guardo los archivos con toda la info del mes (según el directorio en que esté...)
  colnames(data) = c("Fecha", "Hora", "Id_Linea", "Interno", "Dispositivo","SAM_ID", "Cantidad")
  
  data$Id_Linea = as.integer(data$Id_Linea)
  
  data = merge(data, ref_lineas, by.x = "Id_Linea", by.y = "ID_LINEA", all.x = TRUE)

  data = data %>%
    select(Fecha, Hora, Medio, NOMBRE_EMPRESA, Linea_Corregido, Interno, Dispositivo, SAM_ID, Cantidad)
  
  data = data[which(data$Medio == "Colectivo"),]
  
  data$Linea_Corregido = as.integer(data$Linea_Corregido)
  data = data[which(data$Linea_Corregido < 200),]
  
  colnames(data) = c("Fecha", "Hora", "Medio", "Empresa", "Linea", "Interno", "Dispositivo", "SAM_Id", "Cantidad")
  
  directorio_out = "W:\\SUBE\\Unidades en calle\\Merge\\"
  setwd(directorio_out)
  
  nombre_salida = fechas_meses[i,2]
  salida = paste(directorio_out, nombre_salida, sep = "")
  fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  
  # ----------------------------------------------------------------------------------------

}


# --------------------------------------- MERGE ARCHIVOS MENSUALES

directorio = "W:\\SUBE\\Unidades en calle\\Merge\\"
directorio_out = "W:\\SUBE\\"
setwd(directorio)

Allfiles = list.files(pattern = "\\.txt$")



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
    data_merge = rbind(data_merge, data_temp)
  }
  
  vuelta = vuelta +1
}
Sys.time()


data_merge$Cantidad = NULL
data_merge$Dispositivo = NULL
data_merge$SAM_Id = NULL

a = nrow(data_merge)
data_merge = unique(data_merge)
b = nrow(data_merge)
b/a
b-a


data_merge$Cantidad = 1

#warnings()

rm("data_temp")

data_merge = data_merge %>%
  group_by(Fecha, Hora, Medio, Empresa, Linea) %>%
  summarise(Cantidad_Unidades = sum(Cantidad))

colnames(data_merge) = c("Fecha", "Hora", "Medio", "Empresa", "Linea", "Cantidad")

data_merge = data_merge[(data_merge$Fecha >= fecha_corte_merge) & (data_merge$Fecha < fecha_en_curso),]

directorio_out = "W:\\SUBE\\Unidades en calle\\"
setwd(directorio_out)

nombre_salida = "Unidades_En_Calle_SUBE.txt"
salida = paste(directorio_out, nombre_salida, sep = "")
fwrite(data_merge, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
library(googlesheets4)

gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Procesa_Transacciones_gz - Conteo Unidades vs Predictivos"

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
