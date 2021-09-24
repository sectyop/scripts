# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)


file = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQFXa7cH4N6bVMsKCBu9ibL3_dSxZ4rQG4ESKKVbbz5H0Kzg1J54yZxt9Gc-QXbFIPXbYR9ypeBQ2E2/pub?gid=1662905068&single=true&output=csv"

base = read.csv(file,
                encoding = 'UTF-8')

ahora = Sys.time()
fecha = substr(as.character(ahora),1,10)
fecha = gsub("-", "", fecha)
#fecha = (paste(year(ahora), month(ahora), day(ahora), sep = ""))

file_out = paste(fecha,"_Backup_BaseDemorasCGM.txt", sep="")

directorio_out = "W:\\CGM\\BackupDiarioDemoras\\"
setwd(directorio_out)

salida = paste(directorio_out, file_out, sep = "")

fwrite(base, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


# -------------------------------- Ahora agrego los últimos 2 días a la base acumulada -------------------------------

dias = 5
directorio = "W:\\CGM\\"
setwd(directorio)
file = "Demoras_Acumulado.txt"

data = fread(file, 
             header = TRUE, 
             sep = "\t",
             encoding = "UTF-8",
             #colClasses = clases,
             stringsAsFactors = TRUE)

#data$largo = nchar(as.character(data$FechaHora))

data$Fecha_ = dmy_hms(data$FechaHora)

data1 = data[which(is.na(data$Fecha_)),]
data1$Fecha_ = dmy_hm(data1$FechaHora)
data2 = data[which(!(is.na(data$Fecha_))),]

data = rbind(data1, data2)

#data$check = is.na(data$Fecha_)
#data$Fecha_ = ifelse(data$largo >15 , dmy_hms(data$FechaHora), dmy_hm(data$FechaHora))
#data$Fecha_ = as_datetime(data$FechaHora)

#data$Año = year(data$Fecha_)
#data$Mes = month(data$Fecha_)
#data$Dia = day(data$Fecha_)
#data$Hora = hour(data$Fecha_)

base$Fecha_ = dmy_hms(base$FechaHora)
base$Año = year(base$Fecha_)
base$Mes = month(base$Fecha_)
base$Dia = day(base$Fecha_)
base$Hora = hour(base$Fecha_)

fecha_actual = ISOdate(year(now()), month(now()), day(now()), hour = 1)
restar = dias * 24 * 60 * 60

fecha_actual = fecha_actual - restar
#fecha_actual


data = data[which(data$Fecha_ < fecha_actual),]
base = base[which(base$Fecha_ >= fecha_actual),]

#max(data$Fecha_)
#min(base$Fecha_)

nombres_columnas= c("Acceso", "Demora", "Unidad", "Sentido", "FechaHora", "Fecha_", "Anio", "Mes", "Dia", "Hora")
colnames(data) = nombres_columnas
colnames(base) = nombres_columnas

data_out = rbind(data, base)

directorio_out = "W:\\CGM\\"
setwd(directorio_out)

salida = paste(directorio_out, file, sep = "")
fwrite(data_out, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")



# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
library(googlesheets4)

gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "BackupDemorasCGM"

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
