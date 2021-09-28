# Limpio la memoria
rm(list=ls())
gc()

# Cargo librerías

# Package names
packages <- c("lubridate", "data.table", "dplyr", "readxl", "googlesheets4",
              "googledrive")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

drive_auth(email = "datos.ssgm@gmail.com")
gs4_auth(email = "datos.ssgm@gmail.com")

directorio = "W:\\Ecobici\\Bajadas\\Info_Comet\\"
setwd(directorio)
file = "Viajes.csv"

#------------------ DEFINIR LOS MESES QUE SE TRABAJARÁN---------------------------------
cant_meses = 2

diccionario_meses = data.frame(matrix(ncol = 3, nrow = 12))
diccionario_meses$X1 = c(1,2,3,4,5,6,7,8,9,10,11,12)
diccionario_meses$X2 = c("Enero", "Febrero", "Marzo", "Abril", "Mayo",
                         "Junio", "Julio", "Agosto", "Septiembre",
                         "Octubre", "Noviembre", "Diciembre")
diccionario_meses$X3 = c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
colnames(diccionario_meses) = c("Mes_Nro", "Mes_Nombre", "Mes_Txt")

fecha_en_curso = today()

# mes_en_curso = month(today())
# 
# if(mes_en_curso == 1){
#   mes_anterior = 12
#   año_en_curso = year(today())-1
# }else{
#   mes_anterior = mes_en_curso - 1
#   año_en_curso = year(today())
# }
# 
# año_mes_nueva_data = año_en_curso*100 + mes_anterior

fecha_en_curso = today()
mes_en_curso = month(fecha_en_curso)

for (i in (1:cant_meses)){
  mes = month(fecha_en_curso %m-% months(i-1))
  anio = year(fecha_en_curso %m-% months(i-1))
  #mes = month(fecha_en_curso %m-% months(i-1))
  #anio = year(fecha_en_curso %m-% months(i-1))
}

año_mes_nueva_data = anio*100 + mes
#fecha_corte = make_date(anio, j, 1)
#---------------------------------------------------------------------------------------------


clases = c("character", "character", "factor", "factor", "factor", "character", "factor", "factor", "character", 
           "factor", "character", "character", "character", "character", "character", "character", "character", 
           "character", "character", "character", "character", "character", "character", "character")
data = read.delim(file, header = TRUE, fill = TRUE, comment.char = "", colClasses = clases, sep = ",", skipNul = TRUE)

nombres = c("ID", "Estado_cerrado", "Duracion", "Id_de_estacion_de_inicio", "Fecha_de_inicio",
            "Nombre_de_estacion_de_inicio", "Fecha_de_fin", "Id_de_estacion_de_fin_de_viaje", 
            "Nombre_de_estacion_de_fin_de_viaje", "ID_de_ciclista", "Tipo_de_ciclista", "Nombre_de_ciclista", 
            "Apellido_de_ciclista", "Msnbc_de_bicicleta", "Moto_identificador_publico", "Codigo_QR_de_bicicleta", 
            "Modelo_de_bicicleta", "ID_de_factura", "ID_de_linea_de_factura", "Correo_de_ciclista", 
            "Telefono_de_ciclista", "ID_de_producto", "Origen_de_viaje", "Nombre_de_producto")

colnames(data) = nombres

# Elimino las columnas que no sirven
columnas_a_eliminar = c("Duracion", "Id_de_estacion_de_inicio", "Id_de_estacion_de_fin_de_viaje",
                        "Tipo_de_ciclista", "Moto_identificador_publico", "Codigo_QR_de_bicicleta",
                        "Modelo_de_bicicleta", "ID_de_factura", "ID_de_linea_de_factura", "Correo_de_ciclista",
                        "Telefono_de_ciclista")
data[, columnas_a_eliminar] = list(NULL)

#Archivos de referencia
directorio_ref = "W:\\Ecobici\\Referencias\\"

# Guardo el archivo de estaciones y bicicletas
id_estaciones = "1ND4ArLB46dhltTEUg7UBI-9PQDY40pA2IXbBTvvEk2E"
archivo = as_id(id_estaciones)
nombre_archivo = "Caracteristicas_Estaciones.xlsx"

# Descargo el archivo
directorio = "W:\\Ecobici\\Referencias\\"
path_ = paste(directorio, nombre_archivo, sep = "")

drive_download(
  file = archivo,
  path = path_,
  overwrite = TRUE)

ref_base_estaciones = read_xlsx(path = path_,
                                .name_repair = "unique",
                                sheet = "Estaciones")

ref_base_bicicletas = read_xlsx(path = path_,
                                .name_repair = "unique",
                                sheet = "Bicicletas")

ref_base_estaciones = ref_base_estaciones[which(!is.na(ref_base_estaciones$Nombre_Comet)),]
ref_base_bicicletas = ref_base_bicicletas[which(!is.na(ref_base_bicicletas$Msnbc_de_bicicleta)),]

sample = data
rm("data")
names(sample)[names(sample) == 'ID'] = 'ID_Viaje'

# Hay que buscar nuevos nombres de estaciones?
check_estaciones_inicio = unique(sample$Nombre_de_estacion_de_inicio)
check_estaciones_final = unique(sample$Nombre_de_estacion_de_fin_de_viaje)
check_estaciones = data.frame(unique(c(check_estaciones_inicio, check_estaciones_final)))
names(check_estaciones) = "Estaciones"
check_estaciones = merge(check_estaciones, ref_base_estaciones, by.x = "Estaciones", by.y = "Nombre_Comet", all.x = TRUE)
check_estaciones = check_estaciones[is.na(check_estaciones$NOMBRE), 1]
check_estaciones = data.frame(check_estaciones)

# Escrito en el Sheets las nuevas estaciones
celda = "A2:A"
tryCatch({
  range_write(
    ss = id_estaciones,
    data = check_estaciones,
    sheet = 'NUEVAS_Est_Comet',
    range = celda,
    col_names = FALSE,
    reformat = FALSE
  )
}, error=function(e){})

# Incorporo la estacion correcta de inicio y fin a partir de la base de asignación
columnas_a_dejar = c("Nombre_Comet", "NOMBRE", "Lat", "Lon", "DIRECCION", "BARRIO", "COMUNA")
ref_base_estaciones = ref_base_estaciones[, columnas_a_dejar]

sample = merge(sample, ref_base_estaciones, by.x = "Nombre_de_estacion_de_inicio", by.y = "Nombre_Comet", all.x = TRUE)
names(sample)[names(sample) == 'NOMBRE'] = 'Nombre_Inicio_Viaje'
names(sample)[names(sample) == 'DIRECCION'] = 'Direccion_Inicio_Viaje'
names(sample)[names(sample) == 'Lat'] = 'LAT_Inicio_Viaje'
names(sample)[names(sample) == 'Lon'] = 'LON_Inicio_Viaje'
names(sample)[names(sample) == 'BARRIO'] = 'Barrio_Inicio_Viaje'
names(sample)[names(sample) == 'COMUNA'] = 'Comuna_Inicio_Viaje'

sample = merge(sample, ref_base_estaciones, by.x = "Nombre_de_estacion_de_fin_de_viaje", by.y = "Nombre_Comet", all.x = TRUE)
names(sample)[names(sample) == 'NOMBRE'] = 'Nombre_Final_Viaje'
names(sample)[names(sample) == 'DIRECCION'] = 'Direccion_Final_Viaje'
names(sample)[names(sample) == 'Lat'] = 'LAT_Final_Viaje'
names(sample)[names(sample) == 'Lon'] = 'LON_Final_Viaje'
names(sample)[names(sample) == 'BARRIO'] = 'Barrio_Final_Viaje'
names(sample)[names(sample) == 'COMUNA'] = 'Comuna_Final_Viaje'

columnas_a_eliminar = c( "Nombre_de_estacion_de_inicio", "Nombre_de_estacion_de_fin_de_viaje")
sample[, columnas_a_eliminar] = list(NULL)

# Hay que buscar nuevos codigos de bicicletas
check_bicicletas = data.frame(unique(sample$Msnbc_de_bicicleta))
names(check_bicicletas) = "Bicicletas"
check_bicicletas = merge(check_bicicletas, ref_base_bicicletas, by.x = "Bicicletas", by.y = "Msnbc_de_bicicleta", all.x = TRUE)
check_bicicletas = check_bicicletas[is.na(check_bicicletas$Msnbc_de_bicicleta_corregido),]
check_bicicletas = data.frame(check_bicicletas$Bicicletas)

# Escrito en el Sheets las nuevas bicicletas
celda = "A2:A"
tryCatch({
  range_write(
    ss = id_estaciones,
    data = check_bicicletas,
    sheet = 'NUEVAS_Bici_Comet',
    range = celda,
    col_names = FALSE,
    reformat = FALSE
  )
}, error=function(e){})

# Incorporo la bicicleta correcta a partir de la base de asignación
sample = merge(sample, ref_base_bicicletas, by.x = "Msnbc_de_bicicleta", by.y = "Msnbc_de_bicicleta", all.x = TRUE)
sample[, c("Msnbc_de_bicicleta")] = list(NULL)
names(sample)[names(sample) == 'Msnbc_de_bicicleta_corregido'] = 'Msnbc_de_bicicleta'
columnas_a_eliminar = c("Fecha_Agregado")
sample[, columnas_a_eliminar] = list(NULL)

# Incorporo el género a partir de la base de nombres
file_ref = "w:\\Ecobici\\Referencias\\nombres-permitidos.xlsx"
ref_nombres = read_excel(file_ref)

sample$nombre_sin_tilde = chartr("ÁÉÍÓÚ", "AEIOU", toupper(sample$Nombre_de_ciclista))
sample$apellido_sin_tilde = chartr("ÁÉÍÓÚ", "AEIOU", toupper(sample$Apellido_de_ciclista))

sample = merge(sample, ref_nombres, by.x = "nombre_sin_tilde", by.y = "NOMBRE", all.x = TRUE, sort = FALSE)
sample = merge(sample, ref_nombres, by.x = "apellido_sin_tilde", by.y = "NOMBRE", all.x = TRUE, sort = FALSE)
sample$Sexo = ifelse(!is.na(sample$SEXO.x), sample$SEXO.x, 
                   ifelse(!is.na(sample$SEXO.y), sample$SEXO.y,
                          NA))
sample$SEXO.x = NULL
sample$SEXO.y = NULL
sample$ORIGEN.x = NULL
sample$ORIGEN.y = NULL
sample$SIGNIFICADO.x = NULL
sample$SIGNIFICADO.y = NULL
sample$nombre_sin_tilde = NULL
sample$apellido_sin_tilde = NULL

nombres_NA = sample[is.na(sample$Sexo),]

nombres_NA = nombres_NA %>%
  group_by(ID_de_ciclista, Nombre_de_ciclista, Apellido_de_ciclista) %>%
  summarise(count=n())

nombres_NA = nombres_NA[order(- nombres_NA$count),]
nombres_sin_asignar = "W:\\Ecobici\\Referencias\\nombres_en_NA_nuevos.txt"
fwrite(nombres_NA, nombres_sin_asignar, append = TRUE, row.names = FALSE, col.names = TRUE)

# Trabajamos las fechas de inicio y fin
# ...........Inicio: incorporo las variables de tiempo
sample$Fecha_Inicio = ymd_hms(sample$Fecha_de_inicio)
# Año_Inicio = year(Fecha_Inicio)
# Mes_Inicio = month(Fecha_Inicio)
# Dia_Inicio = day(Fecha_Inicio)
# Dia_Semana_Inicio = wday(Fecha_Inicio)  #, label = TRUE, abbr = FALSE)
# Hora_Inicio = hour(Fecha_Inicio)
# AñoMes_Inicio = ifelse(Mes_Inicio < 10, paste(Año_Inicio, "0", Mes_Inicio, sep = ""), paste(Año_Inicio, Mes_Inicio, sep = ""))
# sample = cbind(sample, Fecha_Inicio, Año_Inicio, Mes_Inicio, Dia_Inicio, Dia_Semana_Inicio, Hora_Inicio, AñoMes_Inicio)

# ...........Final: incorporo las variables de tiempo
sample$Fecha_Final = ymd_hms(sample$Fecha_de_fin)
# Año_Final = year(Fecha_Final)
# Mes_Final = month(Fecha_Final)
# Dia_Final = day(Fecha_Final)
# Dia_Semana_Final = wday(Fecha_Final)  #, label = TRUE, abbr = FALSE)
# Hora_Final = hour(Fecha_Final)
# AñoMes_Final = ifelse(Mes_Final < 10, paste(Año_Final, "0", Mes_Final, sep = ""), paste(Año_Final, Mes_Final, sep = ""))
# sample = cbind(sample, Fecha_Final, Año_Final, Mes_Final, Dia_Final, Dia_Semana_Final, Hora_Final, AñoMes_Final)

columnas_a_eliminar = c("Fecha_de_inicio", "Fecha_de_fin")
sample[, columnas_a_eliminar] = list(NULL)

# ...........Duración del viaje (en segundos y minutos) y vemos viajes excedidos
sample$Duracion_Viaje_sec = as.numeric(as.duration(sample$Fecha_Inicio %--% sample$Fecha_Final))
sample$Duracion_Viaje_min = as.numeric(round(sample$Duracion_Viaje_sec / 60, 0))


# Dejo la columna 'Tiempo_Excedido' toda en 1, para no tener problema de mix con el histórico ni de incorporación a BI
sample$Tiempo_Excedido = 1
sample$Contador = ifelse(sample$Estado_cerrado != "", 1, 0)

# Limpio un poco
rm(list=ls()[! ls() %in% c("data", "sample", "año_mes_nueva_data")])
gc()

# Criterios para eliminar viajes
a = nrow(sample)

# prueba = sample %>% group_by(Año_Inicio, Mes_Inicio) %>% summarise(Contador = sum(Contador))

# No consideramos aquellos por debajo de 5 minutos
minutos = 2
sample = sample[which(sample$Duracion_Viaje_sec > (minutos*60)),]
b= nrow(sample)
b/a

sample = sample[which(!(is.na(sample$Msnbc_de_bicicleta))),]
c = nrow(sample)
c/a

sample = sample[which(sample$Contador > 0),]
d = nrow(sample)
d/a

# que_pasa = sample[which(is.na(sample$ID_Inicio_Viaje)),]
# estaciones_sin_id = as.data.frame(unique(que_pasa$Nombre_Inicio_Viaje))
# file_estaciones_sin_id = "W:\\Ecobici\\Referencias\\estaciones_sin_id.txt"
# fwrite(estaciones_sin_id, file_estaciones_sin_id, append = FALSE, row.names = FALSE, col.names = TRUE)
# 
# 
# sample = sample[which(!(is.na(sample$ID_Inicio_Viaje))),]
# e = nrow(sample)
# e/a

# Abro el acumulado histórico
# .......Limpio la memoria
rm(list=ls()[! ls() %in% c("sample", "año_mes_nueva_data")])
gc()

directorio = "W:\\Ecobici\\"
setwd(directorio)
file_acumulado = "Bicicletas_acumulado_procesado_historico.txt"

acumulado = fread(file_acumulado, 
             header = TRUE, 
             sep = "\t")
             #colClasses = clases,
             #encoding = 'UTF-8',
             #stringsAsFactors = TRUE)

acumulado = as.data.frame(acumulado)

# No consideramos aquellos por debajo de 2 minutos
minutos = 2
acumulado = acumulado[which(acumulado$Duracion_Viaje_sec > (minutos*60)),]

#test3 = acumulado %>% group_by(Año_Inicio, Mes_Inicio) %>% summarize(Cantidad = n())
#test = acumulado
#acumulado = test

# sub1 = acumulado[which(acumulado$AñoMes_Inicio >= 202004 & acumulado$AñoMes_Inicio < 202101),]
# sub2 = acumulado[which(acumulado$AñoMes_Inicio < 202004),]
# sub3 = acumulado[which(acumulado$Año_Inicio == 2021),]
# 
# sub1$Fecha_Inicio = as.POSIXct(as.numeric(sub1$Fecha_Inicio), origin = '1970-01-01') + hours(3)
# sub1$Fecha_Final = as.POSIXct(as.numeric(sub1$Fecha_Final), origin = '1970-01-01') + hours(3)
# 
# sub2$Fecha_Inicio = ymd_hms(sub2$Fecha_Inicio)
# sub2$Fecha_Final = ymd_hms(sub2$Fecha_Final)
# 
# sub3$Fecha_Inicio = as.POSIXct(as.numeric(sub3$Fecha_Inicio), origin = '1970-01-01') + hours(3)
# sub3$Fecha_Final = as.POSIXct(as.numeric(sub3$Fecha_Final), origin = '1970-01-01') + hours(3)
# 
# test1 = sub1 %>% group_by(year(sub1$Fecha_Inicio), month(sub1$Fecha_Inicio)) %>% summarize(Cantidad = n())
# test2 = sub2 %>% group_by(year(sub2$Fecha_Inicio), month(sub2$Fecha_Inicio)) %>% summarize(Cantidad = n())
# test3 = sub3 %>% group_by(year(sub3$Fecha_Inicio), month(sub3$Fecha_Inicio)) %>% summarize(Cantidad = n())
# 
# bck = acumulado
# acumulado = rbind(sub1, sub2, sub3)
# test4 = acumulado %>% group_by(year(acumulado$Fecha_Inicio), month(acumulado$Fecha_Inicio)) %>% summarize(Cantidad = n())

# acumulado$Fecha_Inicio = ifelse(is.na(acumulado$Fecha_Inicio),
#                                  make_datetime(year = acumulado$Año_Inicio, 
#                                                month = acumulado$Mes_Inicio, 
#                                                day = acumulado$Dia_Inicio, 
#                                                hour = acumulado$Hora_Inicio, 
#                                                min = 0L,
#                                                sec = 0, 
#                                                tz = "UTC"),
#                                  acumulado$Fecha_Inicio)
# 
# 
# acumulado$Fecha_Final = ifelse(is.na(acumulado$Fecha_Final),
#                                make_datetime(year = acumulado$Año_Inicio, 
#                                              month = acumulado$Mes_Inicio, 
#                                              day = acumulado$Dia_Inicio, 
#                                              hour = acumulado$Hora_Inicio, 
#                                              min = 0L,
#                                              sec = 0, 
#                                              tz = "UTC"),
#                                acumulado$Fecha_Final)
# 
# acumulado$Fecha_Inicio = ymd_hms(acumulado$Fecha_Inicio)
# acumulado$Fecha_Final = ymd_hms(acumulado$Fecha_Final)
# 
# test2 = as.data.frame(acumulado$Fecha_Inicio)
# test2$Anio = year(test2$`acumulado$Fecha_Inicio`)
# test2$Mes = month(test2$`acumulado$Fecha_Inicio`)
# test2 = test2 %>% group_by(Anio, Mes) %>% summarize(Cantidad = n())

#Ordeno las columnas
columnas_preservar = colnames(sample)
acumulado = acumulado[, columnas_preservar]

orden_nombres = colnames(acumulado)
sample = sample[, orden_nombres]

# Elimino el período a incorporar nuevo
fecha_corte = make_date(year = substr(año_mes_nueva_data,1,4), month = substr(año_mes_nueva_data,5,6), day = 1)
acumulado = acumulado[which(acumulado$Fecha_Inicio < fecha_corte),]
sample = sample[which(sample$Fecha_Inicio >= fecha_corte),]

# acumulado$Fecha_Inicio = ymd_hms(acumulado$Fecha_Inicio)
# acumulado$Fecha_Final = ymd_hms(acumulado$Fecha_Final)
acumulado = rbind(acumulado, sample)
acumulado = acumulado[!duplicated(acumulado$ID_Viaje),]

nombre_salida = "Bicicletas_acumulado_procesado_historico.txt"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(acumulado, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# Guardo versión con menos columnas
columnas_a_eliminar = c("ID_de_producto",  "Numero_Inicio_Viaje",  "ID_Inicio_Viaje",  
                        "Anclajes_Inicio_Viaje",  "Vigente_Inicio_Viaje",  "ID_Final_Viaje",  
                        "Direccion_Final_Viaje",  "Numero_Final_Viaje",  "Anclajes_Final_Viaje",   
                        "Vigente_Final_Viaje",  "Fecha_Final",  "Año_Final",  "Mes_Final",  "Dia_Final",  
                        "Dia_Semana_Final",  "Hora_Final",  "AñoMes_Final",  "Duracion_Viaje_sec",  
                        "Direccion_Inicio_Viaje",  "Origen_de_viaje", "Estado_cerrado",  "Apellido_de_ciclista",  
                        "Nombre_de_ciclista",  "ID_Viaje",  "Msnbc_de_bicicleta")

acumulado_menos_columnas = acumulado
acumulado_menos_columnas[, columnas_a_eliminar] = list(NULL)
acumulado_groupby = acumulado_menos_columnas

# Genero un historico acumulado mensual
acumulado_groupby$Duracion_Viaje_min = as.numeric(sub(",", ".", acumulado_groupby$Duracion_Viaje_min, fixed = TRUE))
acumulado_groupby$ID_de_ciclista = as.numeric(acumulado_groupby$ID_de_ciclista)

# Transformo los viajes largos (> 5 horas a un valor fijo)
acumulado_groupby$Duracion_Viaje_min = ifelse(acumulado_groupby$Duracion_Viaje_min > 300, 
                                              NA,
                                              acumulado_groupby$Duracion_Viaje_min)

rm("acumulado", "sample")

# Aca si tengo que ver si o si el tema del tiempo excedido
nombre_archivo = "W:\\Ecobici\\Referencias\\Ref_Abonos.xlsx"

abonos = read_xlsx(
  nombre_archivo,
  sheet = "Hoja1",
  col_names = TRUE,
  skip = 0)

fecha_cambio = ISOdate(2020,5,1, hour = 0)

acumulado_groupby$Dia_Semana = weekdays(acumulado_groupby$Fecha_Inicio)
acumulado_groupby = merge(acumulado_groupby, abonos, by.x = "Nombre_de_producto", by.y = "Nombre", all.x = TRUE)
acumulado_groupby$Tiempo_Uso = ifelse(is.na(acumulado_groupby$Tiempo_Uso),
                                      0,
                                      acumulado_groupby$Tiempo_Uso)


acumulado_groupby$Tiempo_Permitido = ifelse(acumulado_groupby$Fecha_Inicio < fecha_cambio,
                                                   ifelse(acumulado_groupby$Dia_Semana == "sábado" | acumulado_groupby$Dia_Semana == "domingo",
                                                          120,
                                                          60),
                                                   ifelse(acumulado_groupby$Tiempo_Uso == 0,
                                                          30,
                                                          acumulado_groupby$Tiempo_Uso)
                                            )



acumulado_groupby$Tiempo_Excedido_Nro = ifelse(acumulado_groupby$Duracion_Viaje_min > acumulado_groupby$Tiempo_Permitido,
                                               acumulado_groupby$Duracion_Viaje_min - acumulado_groupby$Tiempo_Permitido,
                                               0)

acumulado_groupby$Tiempo_Excedido = ifelse(acumulado_groupby$Tiempo_Excedido_Nro == 0, 
                                           0,
                                           1)

acumulado_groupby$Duracion_Viaje_Agrupado = ifelse(acumulado_groupby$Tiempo_Excedido_Nro == 0, "Buen uso",
                                                   ifelse(acumulado_groupby$Tiempo_Excedido_Nro < 5, "Gracia", 
                                                          "Exceso"))

# Llevo a multiplos de 5min
# acumulado_groupby$Duracion_Viaje_min = floor(acumulado_groupby$Duracion_Viaje_min / 5) * 5

# Hago el group by
acumulado_groupby$Fecha_Inicio = floor_date(acumulado_groupby$Fecha_Inicio, unit = "hour")
usuarios_groupby = acumulado_groupby

acumulado_groupby = acumulado_groupby %>% group_by(Fecha_Inicio,
                                          Barrio_Inicio_Viaje, Comuna_Inicio_Viaje,
                                          LAT_Inicio_Viaje, LON_Inicio_Viaje, 
                                          Nombre_Inicio_Viaje, 
                                          Tiempo_Permitido,
                                          Duracion_Viaje_Agrupado) %>%
                                 summarise(Contador = sum(Contador),
                                           Duracion_Viaje_min = mean(Duracion_Viaje_min, na.rm = TRUE))

usuarios_groupby = usuarios_groupby %>% select(Fecha_Inicio,
                                           Nombre_Inicio_Viaje, 
                                           Sexo, 
                                           ID_de_ciclista)


nombre_salida = "Bicicletas_acumulado_procesado_agrupado.txt"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")
fwrite(acumulado_groupby, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

nombre_salida = "Usuarios_acumulado_procesado_agrupado.txt"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")
fwrite(usuarios_groupby, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


# Me quedo los ultimos 12 meses y guardo el archivo de trabajo
fecha_corte = ISOdate(2020,1,1, hour=0)
acumulado_corte = acumulado_menos_columnas[which(acumulado_menos_columnas$Fecha_Inicio >= fecha_corte),]

nombre_salida = "Bicicletas_acumulado_procesado.txt"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")
fwrite(acumulado_corte, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Anexa Nueva Bajada"

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

