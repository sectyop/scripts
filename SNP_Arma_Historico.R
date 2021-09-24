# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(readxl)
library(googlesheets4)

directorio = "W:\\Agentes Viales\\No Programados\\Backup\\"
setwd(directorio)

archivos = list.files(pattern = "\\.xlsx")

# Por lo que se ve, los archivos tienen 24 columnas (sacando las de fórmulas que vamos a descartar)
data = data.frame(matrix(ncol = 22, nrow = 0))

#file = "Registro Diario CGM - CUCC (BU 17Jun20).xlsx"

clases = c("numeric", "date", "date", "text", "text",
           "text", "text", "text", "text", "text", "text", "text",
           "text", "text", "text", "text", "date",
           "text", "text", "text", "date", "date",
           "text", "text", "guess", "guess", "guess",
           "guess", "guess", "guess", "guess", "guess")

clases = c('numeric', 'date', 'date', 'text', 'text', 'text', 'text', 'text', 'text', 
           'text', 'text', 'text', 'text', 'text', 'text', 'text', 'date', 'text', 
           'text', 'text', 'date', 'date')

for (file in archivos){
  #file = archivos[1]
  data_temp = read_xlsx(file,
                        .name_repair = "unique",
                        range = "A4:V90000",
                        sheet = "Datos Operativos",
                        col_types = clases)
                        # skip = 3)
  
  data_temp = data_temp[which(!is.na(data_temp$`Fecha Nec`)),(1:22)]
  
  # Corrijo todos los campos de fecha y hora
  data_temp$`Fecha Nec` = as.IDate(data_temp$`Fecha Nec`)
  
  data_temp$`Hora Recep` = hour(data_temp$`Hora Recep`)/24 + minute(data_temp$`Hora Recep`)/(24*60)
  data_temp$`Hora Orden Conf` = hour(data_temp$`Hora Orden Conf`)/24 + minute(data_temp$`Hora Orden Conf`)/(24*60)
  data_temp$`Hora Llegada` = hour(data_temp$`Hora Llegada`)/24 + minute(data_temp$`Hora Llegada`)/(24*60)
  data_temp$`Hora Retiro Ag Pto` = hour(data_temp$`Hora Retiro Ag Pto`)/24 + minute(data_temp$`Hora Retiro Ag Pto`)/(24*60)
  
  # Corrijo el tipo de dato del número de suceso
  data_temp$`Nº Inc` = as.integer(data_temp$`Nº Inc`)
  
  data = rbind(data, data_temp)
  
}

rm("data_temp")

# Guardo el Histórico
file_out = "Historico_SNP.txt"
directorio_out = "W:\\Agentes Viales\\No Programados\\"
salida = paste(directorio_out, file_out, sep = "")

fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "nacho.ls@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "SNP_Arma_Historico"

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

