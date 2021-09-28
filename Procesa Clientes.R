# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)
library(googlesheets4)


# Abro el historico

directorio = "W:\\Ecobici\\Clientes\\"

setwd(directorio)

file = "Base_Clientes.txt"

data_historico = fread(file, 
                       header = TRUE, 
                       sep = "\t", 
                       encoding = "UTF-8")

# Abro el nuevo archivo

directorio = "W:\\Ecobici\\Clientes\\Bajadas\\"

setwd(directorio)

file = "Clientes.csv"

cleanFiles<-function(file,newfile){
  writeLines(iconv(readLines(file,skipNul = TRUE)),newfile)
}

newfile = "W:\\Ecobici\\Clientes\\Historico\\Clientes_ref.csv"
cleanFiles(file, newfile)

data_nuevo = fread(newfile, 
                   header = TRUE, 
                   sep = ",", 
                   encoding = "Latin-1")


data_historico$Fecha_de_nacimiento = ymd(data_historico$Fecha_de_nacimiento)

data_nuevo$Fecha_de_nacimiento = ymd(data_nuevo$`Fecha de nacimiento`)
data_nuevo$Inicio_Suscripcion = ymd_hms(data_nuevo$`Inicio de suscripción actual (o última)`)
data_nuevo$Fin_Suscripcion = ymd_hms(data_nuevo$`Final de suscripción actual (o última)`)

# Elimino las columnas que no sirven

columnas_a_eliminar = c("Teléfono1", "Teléfono2", "Correo electrónico", "Código postal", "Código postal de facturación",
                        "Personalizado1", "Personalizado3", "ID de producto de suscripción actual (o última)",
                        "Renovación automática de suscripción actual (o última)", "Número de bicicletas de suscripción actual (o última)",
                        "Código de barras de llave", "Tarjeta de crédito", "Opc en correos de marketing", "Opc en sms de marketing",
                        "Opc en correos de marketing como nivel", "Opc en sms de marketing como nivel", "Num. de identificación nacional",
                        "Tipo de identificación nacional","Fecha de nacimiento", "Inicio de suscripción actual (o última)",
                        "Final de suscripción actual (o última)")

data_nuevo[, columnas_a_eliminar] = list(NULL)

data_nuevo$Personalizado2 = substr(data_nuevo$Personalizado2, 3, 10)

# Corrijo nombres de columnas


nombres_columnas = colnames(data_historico)
colnames(data_nuevo) = nombres_columnas

# Los uno en un único dataset

data = rbind(data_nuevo, data_historico)

# Limpio un poco
rm(list=ls()[! ls() %in% c("data")])
gc()

# Elimino duplicados

# Puede pasar que bajo un mismo id_cliente haya varios registros. En general corresponde a la misma persona bajo múltiples activaciones
# Entonces lo que vamos a hacer es ordenar por Inicio_Suscripción (descendiente) y ahí nos quedamos con el registro más actual

# Ordeno los datos por id_cliente e inicio_suscripcion
data = data[order(data$Id_Cliente, -data$Inicio_Suscripcion),]

# Simplifico un poco la cardinalidad del DNI. Me quedo únicamente con el valor / 1.000.000 (si es > 90 --> extranjero)
data$DNI = as.integer(data$DNI)
data$DNI = ifelse(data$DNI < 1000000, NA, data$DNI)
data$DNI = round(data$DNI / 1000000, 0)

# Elimino duplicados por id_cliente

a = nrow(data)

data = data %>% 
  distinct(Id_Cliente, .keep_all = T)

b = nrow(data)

b/a

# Guardo el archivo de salida
nombre_salida = "Base_Clientes.txt"
directorio = "W:\\Ecobici\\Clientes\\"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

#----------------------------------------------------------------------------------

# Ahora vamos a procesar la historia de las bicicletas

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Abro la bajada nueva
directorio = "W:\\Ecobici\\Salud_Bicicletas\\Bajadas\\"
setwd(directorio)

file = "Bicicletas.csv"

data = iconv(readLines(file,skipNul = TRUE))
data_nuevo = read.table(text = paste(data, collapse='\n'), 
                        header = TRUE, 
                        stringsAsFactors = FALSE, 
                        sep=',')

# Si es fin de mes, muevo el archivo a la carpeta de historico
my.file.rename = function(from, to) {
  todir <- dirname(to)
  if (!isTRUE(file.info(todir)$isdir)) dir.create(todir, recursive=TRUE)
  #file.rename(from = from,  to = to)
  file.copy(from = from,  to = to)
}

fecha = today() + 1
dia_del_mes = day(fecha)

fecha_min = as.Date(min(data_nuevo$Start.Date.of.the.state))
fecha_max = as.Date(today())

fecha_min = format(fecha_min, "%Y%m%d")
fecha_max = format(fecha_max, "%Y%m%d")

nombre_archivo = paste("W:/Ecobici/Salud_Bicicletas/Old/", 
                       fecha_min, 
                       " a ", 
                       fecha_max, 
                       ".csv", 
                       sep = "")

if(dia_del_mes == 1){
  my.file.rename(from = "W:/Ecobici/Salud_Bicicletas/Bajadas/Bicicletas.csv",
                 to = nombre_archivo)
}

# Abro las bajadas historicas
directorio = "W:\\Ecobici\\Salud_Bicicletas\\Old\\"
setwd(directorio)
Allfiles = list.files(pattern = "\\.csv$")
vuelta = 1

Sys.time()
for (file in Allfiles){
  if(vuelta < 2){
    print(c(file))
    data = iconv(readLines(file,skipNul = TRUE))
    
    data_ = read.table(text = paste(data, collapse='\n'), 
                       header = TRUE, 
                       stringsAsFactors = FALSE, 
                       sep=',')
    #nombres = colnames(data)
  }
  
  if(vuelta > 1){
    print(c(file))
    data = iconv(readLines(file,skipNul = TRUE))
    
    data_temp = read.table(text = paste(data, collapse='\n'), 
                           header = TRUE, 
                           stringsAsFactors = FALSE, 
                           sep=',')
    #colnames(data_temp) = nombres
    data_ = rbind(data_, data_temp)
  }
  vuelta = vuelta +1
}
Sys.time()

rm("data_temp")

# Uno ambos df
data = rbind(data_nuevo, data_)

rm("data_")
rm("data_nuevo")

procesa_salud_bicicletas = function(data_nuevo){
  # Elimino ID_Bicicleta = NULL
  data_nuevo = data_nuevo[!is.na(data_nuevo$Asset.ID),]
  
  # Corrijo el formato de fechas
  data_nuevo$Start.Date.of.the.state = ymd_hms(data_nuevo$Start.Date.of.the.state)
  data_nuevo$End.Date.of.the.state = ymd_hms(data_nuevo$End.Date.of.the.state)
  
  # Ordeno los datos por bicicleta y start_date
  data_nuevo = data_nuevo[order(data_nuevo$Asset.ID, data_nuevo$Start.Date.of.the.state),]
  
  # Elimino los estados con Duration = 0
  data_nuevo = data_nuevo[which(data_nuevo$Duration>0),]
  
  # Copio el df desde la segunda fila, para en todos los casos quedarme con el registro siguiente (estado de destino)
  largo = nrow(data_nuevo)
  
  col_aux = data_nuevo[2:largo, 2:6]
  col_aux = add_row(col_aux)
  
  # Elimino columnas innecesarias
  data_nuevo$Duration = NULL
  col_aux$Duration = NULL
  
  # Corrijo nombres columnas
  colnames(data_nuevo) = c("Asset_Type", "ID_Bicicleta", "Fecha_Comienzo_1", "Fecha_Final_1", "Estado_Comienzo")
  colnames(col_aux) = c("ID_Bicicleta_Aux", "Fecha_Comienzo_2", "Fecha_Final_2", "Estado_Final")
  
  # Arreglo el índice de data_nuevo
  rownames(data_nuevo) = NULL
  
  # Uno los df
  data_nuevo = cbind(data_nuevo, col_aux)
  
  # Arreglo los estados cuando los ID son diferentes
  data_nuevo$Estado_Final = ifelse(data_nuevo$ID_Bicicleta != data_nuevo$ID_Bicicleta_Aux, NA, data_nuevo$Estado_Final)
  data_nuevo$Fecha_Final_1 = ifelse(data_nuevo$ID_Bicicleta != data_nuevo$ID_Bicicleta_Aux, NA, data_nuevo$Fecha_Final_1)
  
  # Corrijo la fecha final
  data_nuevo$Fecha_Final_1 = as.POSIXct(data_nuevo$Fecha_Final_1, origin = '1970-01-01', tz = 'UTC')
  
  # Arreglo los estados cuando los ID son diferentes ------------
  data_nuevo$Fecha_Final = ifelse(data_nuevo$Estado_Comienzo == data_nuevo$Estado_Final, data_nuevo$Fecha_Final_2, data_nuevo$Fecha_Final_1)
  data_nuevo$Fecha_Final = as.POSIXct(data_nuevo$Fecha_Final, origin = '1970-01-01', tz = 'UTC')
  
  # Cambio Estado_Final = NA a Otra cosa
  data_nuevo$Estado_Final = ifelse(is.na(data_nuevo$Estado_Final), "Poner_NA", data_nuevo$Estado_Final)
  
  # Elimino las filas donde coinciden estado inicial y final
  data_nuevo = data_nuevo[!(data_nuevo$Estado_Comienzo == data_nuevo$Estado_Final),]
  
  # Arreglo el índice de data_nuevo
  rownames(data_nuevo) = NULL
  
  
  # Ahora tengo que evaluar pero ahora con el dato anterior
  largo = nrow(data_nuevo)
  
  col_aux_fecha_final = data.frame(data_nuevo[1:largo-1, 10])
  col_aux_fecha_final = add_row(col_aux_fecha_final, .before = 1)
  
  colnames(col_aux_fecha_final) = c("Fecha_Auxiliar")
  
  data_nuevo = cbind(data_nuevo, col_aux_fecha_final)
  
  # Corrijo entonces la fecha inicial en estos casos
  
  data_nuevo$Fecha_Comienzo = ifelse((data_nuevo$Fecha_Comienzo_1 > data_nuevo$Fecha_Auxiliar) & !is.na(data_nuevo$Fecha_Auxiliar), 
                                     data_nuevo$Fecha_Auxiliar, data_nuevo$Fecha_Comienzo_1)
  data_nuevo$Fecha_Comienzo = as.POSIXct(data_nuevo$Fecha_Comienzo, origin = '1970-01-01', tz = 'UTC')
  
  
  data_nuevo = data_nuevo[, c("Asset_Type", "ID_Bicicleta", "Fecha_Comienzo", "Fecha_Final", "Estado_Comienzo", "Estado_Final")]
  
  # Vuelvo a poner NA en el EstadoFinal
  data_nuevo$Estado_Final = ifelse(data_nuevo$Estado_Final == "Poner_NA", NA, data_nuevo$Estado_Final)
  
  # Calculo la duración
  data_nuevo$Duracion_sec = as.integer(data_nuevo$Fecha_Final - data_nuevo$Fecha_Comienzo)
  
  return(data_nuevo)
}

data = procesa_salud_bicicletas(data)

# # Abro el historico
# 
# directorio = "W:\\Ecobici\\Salud_Bicicletas\\"
# 
# setwd(directorio)
# 
# file = "Historico_Salud_Bicicletas.txt"
# 
# data_historico = fread(file, 
#                        header = TRUE, 
#                        sep = "\t")
# 
# 
# # Uno ambos archivos
# 
# data = rbind(data_nuevo, data_historico)
# 
# # Ordeno los datos por bicicleta y start_date
# data = data[order(data$ID_Bicicleta, data$Fecha_Comienzo),]
# rownames(data) = NULL
# 

# Elimino duplicados por ID y Fecha_Comienzo
data2 = data
a = nrow(data)
data = data %>% 
            distinct(ID_Bicicleta, Fecha_Comienzo, .keep_all = T)
b = nrow(data)

b/a

rownames(data) = NULL

# Guardo el archivo de salida
nombre_salida = "Historico_Salud_Bicicletas.txt"
directorio = "W:\\Ecobici\\Salud_Bicicletas\\"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


#----------------------------------------------------------------------------------

# Ahora vamos a procesar la historia de las estaciones

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Abro la bajada nueva
directorio = "W:\\Ecobici\\Salud_Estaciones\\Bajadas\\"

setwd(directorio)

file = "Estaciones.csv"
#file = "Estados_salud_estaciones_total.csv"

#cleanFiles<-function(file,newfile){
#  writeLines(iconv(readLines(file,skipNul = TRUE)),newfile)
#}

#newfile = "W:\\Ecobici\\Salud_Bicicletas\\Bajadas\\Aux.csv"

#cleanFiles(file, newfile)
data = iconv(readLines(file,skipNul = TRUE))

data_nuevo = read.table(text = paste(data, collapse='\n'), 
                        header = TRUE, 
                        stringsAsFactors = FALSE, 
                        sep=',')

rm("data")

# Elimino ID_Bicicleta = NULL
data_nuevo = data_nuevo[!is.na(data_nuevo$Asset.ID),]

# Corrijo el formato de fechas
data_nuevo$Start.Date.of.the.state = ymd_hms(data_nuevo$Start.Date.of.the.state)
data_nuevo$End.Date.of.the.state = ymd_hms(data_nuevo$End.Date.of.the.state)

# Ordeno los datos por bicicleta y start_date
data_nuevo = data_nuevo[order(data_nuevo$Asset.ID, data_nuevo$Start.Date.of.the.state),]

# Elimino los estados con Duration = 0
data_nuevo = data_nuevo[which(data_nuevo$Duration>0),]

# Copio el df desde la segunda fila, para en todos los casos quedarme con el registro siguiente (estado de destino)
largo = nrow(data_nuevo)

col_aux = data_nuevo[2:largo, 2:6]
col_aux = add_row(col_aux)

# Elimino columnas innecesarias
data_nuevo$Duration = NULL
col_aux$Duration = NULL

# Corrijo nombres columnas
colnames(data_nuevo) = c("Asset_Type", "ID_Estacion", "Fecha_Comienzo_1", "Fecha_Final_1", "Estado_Comienzo")
colnames(col_aux) = c("ID_Estacion_Aux", "Fecha_Comienzo_2", "Fecha_Final_2", "Estado_Final")

# Arreglo el índice de data_nuevo
rownames(data_nuevo) = NULL

# Uno los df
data_nuevo = cbind(data_nuevo, col_aux)

# Arreglo los estados cuando los ID son diferentes
data_nuevo$Estado_Final = ifelse(data_nuevo$ID_Estacion != data_nuevo$ID_Estacion_Aux, NA, data_nuevo$Estado_Final)
data_nuevo$Fecha_Final_1 = ifelse(data_nuevo$ID_Estacion != data_nuevo$ID_Estacion_Aux, NA, data_nuevo$Fecha_Final_1)

# Corrijo la fecha final
data_nuevo$Fecha_Final_1 = as.POSIXct(data_nuevo$Fecha_Final_1, origin = '1970-01-01', tz = 'UTC')

# Arreglo los estados cuando los ID son diferentes ------------
data_nuevo$Fecha_Final = ifelse(data_nuevo$Estado_Comienzo == data_nuevo$Estado_Final, data_nuevo$Fecha_Final_2, data_nuevo$Fecha_Final_1)
data_nuevo$Fecha_Final = as.POSIXct(data_nuevo$Fecha_Final, origin = '1970-01-01', tz = 'UTC')

# Cambio Estado_Final = NA a Otra cosa
data_nuevo$Estado_Final = ifelse(is.na(data_nuevo$Estado_Final), "Poner_NA", data_nuevo$Estado_Final)

# Elimino las filas donde coinciden estado inicial y final
data_nuevo = data_nuevo[!(data_nuevo$Estado_Comienzo == data_nuevo$Estado_Final),]

# Arreglo el índice de data_nuevo
rownames(data_nuevo) = NULL


# Ahora tengo que evaluar pero ahora con el dato anterior
largo = nrow(data_nuevo)

col_aux_fecha_final = data.frame(data_nuevo[1:largo-1, 10])
col_aux_fecha_final = add_row(col_aux_fecha_final, .before = 1)

colnames(col_aux_fecha_final) = c("Fecha_Auxiliar")

data_nuevo = cbind(data_nuevo, col_aux_fecha_final)

# Corrijo entonces la fecha inicial en estos casos

data_nuevo$Fecha_Comienzo = ifelse((data_nuevo$Fecha_Comienzo_1 > data_nuevo$Fecha_Auxiliar) & !is.na(data_nuevo$Fecha_Auxiliar), 
                                   data_nuevo$Fecha_Auxiliar, data_nuevo$Fecha_Comienzo_1)
data_nuevo$Fecha_Comienzo = as.POSIXct(data_nuevo$Fecha_Comienzo, origin = '1970-01-01', tz = 'UTC')


data_nuevo = data_nuevo[, c("Asset_Type", "ID_Estacion", "Fecha_Comienzo", "Fecha_Final", "Estado_Comienzo", "Estado_Final")]

# Vuelvo a poner NA en el EstadoFinal
data_nuevo$Estado_Final = ifelse(data_nuevo$Estado_Final == "Poner_NA", NA, data_nuevo$Estado_Final)

# Calculo la duración
data_nuevo$Duracion_sec = as.integer(data_nuevo$Fecha_Final - data_nuevo$Fecha_Comienzo)



# Abro el historico

directorio = "W:\\Ecobici\\Salud_Estaciones\\"

setwd(directorio)

file = "Historico_Salud_Estaciones.txt"

data_historico = fread(file, 
                       header = TRUE, 
                       sep = "\t")


# Uno ambos archivos

data = rbind(data_historico, data_nuevo)

# Ordeno los datos por bicicleta y start_date
data = data[order(data$ID_Estacion, data$Fecha_Comienzo),]
rownames(data) = NULL


# Elimino duplicados por ID y Fecha_Comienzo

a = nrow(data)

data = data %>% 
  distinct(ID_Estacion, Fecha_Comienzo, .keep_all = T)

b = nrow(data)

b/a

rownames(data) = NULL

# Guardo el archivo de salida
nombre_salida = "Historico_Salud_Estaciones.txt"
directorio = "W:\\Ecobici\\Salud_Estaciones\\"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


#----------------------------------------------------------------------------------

# Ahora vamos a procesar la historia de los anclajes

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Abro la bajada nueva
directorio = "W:\\Ecobici\\Salud_Anclajes\\Bajadas\\"

setwd(directorio)

file = "Anclajes.csv"
#file = "Estados_anclajes_total.csv"

#cleanFiles<-function(file,newfile){
#  writeLines(iconv(readLines(file,skipNul = TRUE)),newfile)
#}

#newfile = "W:\\Ecobici\\Salud_Bicicletas\\Bajadas\\Aux.csv"

#cleanFiles(file, newfile)
data = iconv(readLines(file,skipNul = TRUE))

data_nuevo = read.table(text = paste(data, collapse='\n'), 
                        header = TRUE, 
                        stringsAsFactors = FALSE, 
                        sep=',')

rm("data")

# Elimino ID_Bicicleta = NULL
data_nuevo = data_nuevo[!is.na(data_nuevo$Asset.ID),]

# Corrijo el formato de fechas
data_nuevo$Start.Date.of.the.state = ymd_hms(data_nuevo$Start.Date.of.the.state)
data_nuevo$End.Date.of.the.state = ymd_hms(data_nuevo$End.Date.of.the.state)

# Ordeno los datos por bicicleta y start_date
data_nuevo = data_nuevo[order(data_nuevo$Asset.ID, data_nuevo$Start.Date.of.the.state),]

# Elimino los estados con Duration = 0
data_nuevo = data_nuevo[which(data_nuevo$Duration>0),]

# Copio el df desde la segunda fila, para en todos los casos quedarme con el registro siguiente (estado de destino)
largo = nrow(data_nuevo)

col_aux = data_nuevo[2:largo, 2:6]
col_aux = add_row(col_aux)

# Elimino columnas innecesarias
data_nuevo$Duration = NULL
col_aux$Duration = NULL

# Corrijo nombres columnas
colnames(data_nuevo) = c("Asset_Type", "ID_Anclajes", "Fecha_Comienzo_1", "Fecha_Final_1", "Estado_Comienzo")
colnames(col_aux) = c("ID_Anclajes_Aux", "Fecha_Comienzo_2", "Fecha_Final_2", "Estado_Final")

# Arreglo el índice de data_nuevo
rownames(data_nuevo) = NULL

# Uno los df
data_nuevo = cbind(data_nuevo, col_aux)

# Arreglo los estados cuando los ID son diferentes
data_nuevo$Estado_Final = ifelse(data_nuevo$ID_Anclajes != data_nuevo$ID_Anclajes_Aux, NA, data_nuevo$Estado_Final)
data_nuevo$Fecha_Final_1 = ifelse(data_nuevo$ID_Anclajes != data_nuevo$ID_Anclajes_Aux, NA, data_nuevo$Fecha_Final_1)

# Corrijo la fecha final
data_nuevo$Fecha_Final_1 = as.POSIXct(data_nuevo$Fecha_Final_1, origin = '1970-01-01', tz = 'UTC')

# Arreglo los estados cuando los ID son diferentes ------------
data_nuevo$Fecha_Final = ifelse(data_nuevo$Estado_Comienzo == data_nuevo$Estado_Final, data_nuevo$Fecha_Final_2, data_nuevo$Fecha_Final_1)
data_nuevo$Fecha_Final = as.POSIXct(data_nuevo$Fecha_Final, origin = '1970-01-01', tz = 'UTC')

# Cambio Estado_Final = NA a Otra cosa
data_nuevo$Estado_Final = ifelse(is.na(data_nuevo$Estado_Final), "Poner_NA", data_nuevo$Estado_Final)

# Elimino las filas donde coinciden estado inicial y final
data_nuevo = data_nuevo[!(data_nuevo$Estado_Comienzo == data_nuevo$Estado_Final),]

# Arreglo el índice de data_nuevo
rownames(data_nuevo) = NULL


# Ahora tengo que evaluar pero ahora con el dato anterior
largo = nrow(data_nuevo)

col_aux_fecha_final = data.frame(data_nuevo[1:largo-1, 10])
col_aux_fecha_final = add_row(col_aux_fecha_final, .before = 1)

colnames(col_aux_fecha_final) = c("Fecha_Auxiliar")

data_nuevo = cbind(data_nuevo, col_aux_fecha_final)

# Corrijo entonces la fecha inicial en estos casos

data_nuevo$Fecha_Comienzo = ifelse((data_nuevo$Fecha_Comienzo_1 > data_nuevo$Fecha_Auxiliar) & !is.na(data_nuevo$Fecha_Auxiliar), 
                                   data_nuevo$Fecha_Auxiliar, data_nuevo$Fecha_Comienzo_1)
data_nuevo$Fecha_Comienzo = as.POSIXct(data_nuevo$Fecha_Comienzo, origin = '1970-01-01', tz = 'UTC')


data_nuevo = data_nuevo[, c("Asset_Type", "ID_Anclajes", "Fecha_Comienzo", "Fecha_Final", "Estado_Comienzo", "Estado_Final")]

# Vuelvo a poner NA en el EstadoFinal
data_nuevo$Estado_Final = ifelse(data_nuevo$Estado_Final == "Poner_NA", NA, data_nuevo$Estado_Final)

# Calculo la duración
data_nuevo$Duracion_sec = as.integer(data_nuevo$Fecha_Final - data_nuevo$Fecha_Comienzo)



# Abro el historico

directorio = "W:\\Ecobici\\Salud_Anclajes\\"

setwd(directorio)

file = "Historico_Salud_Anclajes.txt"

data_historico = fread(file, 
                       header = TRUE, 
                       sep = "\t")


# Uno ambos archivos

data = rbind(data_historico, data_nuevo)

# Ordeno los datos por bicicleta y start_date
data = data[order(data$ID_Anclajes, data$Fecha_Comienzo),]
rownames(data) = NULL


# Elimino duplicados por ID y Fecha_Comienzo

a = nrow(data)

data = data %>% 
  distinct(ID_Anclajes, Fecha_Comienzo, .keep_all = T)

b = nrow(data)

b/a

rownames(data) = NULL

# Guardo el archivo de salida
nombre_salida = "Historico_Salud_Anclajes.txt"
directorio = "W:\\Ecobici\\Salud_Anclajes\\"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


# ------------------ ARMO UN UNICO ARCHIVO CON BICICLETAS, ESTACIONES Y ANCLAJES -------------------------------
# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

directorio = "W:\\Ecobici\\Salud_Bicicletas\\"
setwd(directorio)
file = "Historico_Salud_Bicicletas.txt"

data_historico_bike = fread(file, 
                       header = TRUE, 
                       sep = "\t")

directorio = "W:\\Ecobici\\Salud_Anclajes\\"
setwd(directorio)
file = "Historico_Salud_Anclajes.txt"

data_historico_dock = fread(file, 
                       header = TRUE, 
                       sep = "\t")

directorio = "W:\\Ecobici\\Salud_Estaciones\\"
setwd(directorio)
file = "Historico_Salud_Estaciones.txt"

data_historico_station = fread(file, 
                       header = TRUE, 
                       sep = "\t")

nombres_columnas = c("Asset_Type", "ID", "Fecha_Comienzo", "Fecha_Final", "Estado_Comienzo", "Estado_Final", "Duracion_sec")
colnames(data_historico_bike) = nombres_columnas
colnames(data_historico_dock) = nombres_columnas
colnames(data_historico_station) = nombres_columnas

data = rbind(data_historico_bike, data_historico_dock, data_historico_station)

# Guardo el archivo de salida
nombre_salida = "Historico_Salud_Todos.txt"
directorio = "W:\\Ecobici\\Operacion\\"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


#----------------------------------------------------------------------------------

# Ahora vamos a procesar la disponibilidad (full, empty) de las estaciones

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Abro la bajada nueva
directorio = "W:\\Ecobici\\Bajadas\\Info_Comet\\"

setwd(directorio)

file = "Disponibilidad.csv"

#cleanFiles(file, newfile)
data = iconv(readLines(file,skipNul = TRUE))

data_nuevo = read.table(text = paste(data, collapse='\n'), 
                        header = TRUE, 
                        stringsAsFactors = FALSE, 
                        sep=',')
rm("data")

# Abro el historico
directorio = "W:\\Ecobici\\Operacion\\"
setwd(directorio)
file = "Disponibilidad_historico.csv"

data = iconv(readLines(file,skipNul = TRUE))

data_historico = read.table(text = paste(data, collapse='\n'), 
                            header = TRUE, 
                            stringsAsFactors = FALSE, 
                            sep=',')

rm("data")

# Uno ambos archivos
data = rbind(data_historico, data_nuevo)


# Elimino duplicados por ID y Fecha_Comienzo

a = nrow(data)

data = data %>% 
  distinct(Station.Id, Start, .keep_all = T)

b = nrow(data)

b/a

nombres_columnas = c('Station Id', 'Station Name', 'Shortage Type', 'Start', 'End', 'Duration', 'Docks count at start', 
                     'Docks count at end', 'Available docks', 'Station nominal dock capacity', 'Dock occupancy at start', 
                     'Time credits granted', 'Station Group Id', 'Station Group Name', 'Station Cluster Id', 
                     'Station Cluster Name', 'Station Crown Id', 'Station Crown Name')

colnames(data) = nombres_columnas

# Guardo el archivo de salida
nombre_salida = "Disponibilidad_historico_procesado.txt"
directorio = "W:\\Ecobici\\Operacion\\"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

rm("data")
rm("data_historico")
rm("data_nuevo")



# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Procesa Clientes"

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