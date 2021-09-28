# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(googledrive)
library(readxl)
library(data.table)
library(tidyverse)
library(lubridate)
library(googlesheets4)


# Busco archivos con el nombre de Compilado * .rar
drive_auth(email = "datos.ssgm@gmail.com")
id_compilado = drive_get("Compilado.txt")$id
archivo = as_id(id_compilado)
nombre_archivo = "Compilado_facturas_tembici.txt"

# Descargo el archivo
directorio = "W:\\Ecobici\\Facturas\\"
path_ = paste(directorio, nombre_archivo, sep = "")

drive_download(
  file = archivo,
  path = path_,
  overwrite = TRUE)

# Leo el archivo y lo guardo
setwd(directorio)
data = fread(nombre_archivo,
             encoding = "UTF-8")

nombres_columnas = c("Numero", "Company Account", "Merchant Account", "PrimeroDePsp Reference", "Merchant Reference", 
                     "PrimeroDePayment Method", "PrimeroDePayment Method Variant", "PrimeroDeCreation Date", "PrimeroDeTimeZone", 
                     "PrimeroDeAmount", "PrimeroDeType", "PrimeroDeRisk Scoring", "PrimeroDeShopper Interaction", "PrimeroDeShopper Name", 
                     "PrimeroDeShopper PAN", "PrimeroDeShopper IP", "PrimeroDeShopper Country", "PrimeroDeIssuer Name", "PrimeroDeIssuer Id", 
                     "PrimeroDeIssuer City", "PrimeroDeIssuer Country", "PrimeroDeAcquirer Response", "PrimeroDeRaw acquirer response", 
                     "PrimeroDeAuthorisation Code", "PrimeroDeShopper Email", "PrimeroDeShopper Reference", "PrimeroDe3D Directory Response", 
                     "PrimeroDe3D Authentication Response", "PrimeroDeCVC2 Response", "PrimeroDeAVS Response", "PrimeroDeBilling Street", 
                     "PrimeroDeBilling House Number / Name", "PrimeroDeBilling City", "PrimeroDeBilling Postal Code / ZIP", 
                     "PrimeroDeBilling State / Province", "PrimeroDeBilling Country", "PrimeroDeDelivery Street", 
                     "PrimeroDeDelivery House Number / Name", "PrimeroDeDelivery City", "PrimeroDeDelivery Postal Code / ZIP", 
                     "PrimeroDeDelivery State / Province", "PrimeroDeDelivery Country", "PrimeroDeAcquirer Reference", 
                     "PrimeroDeBIN Funding Source", "PrimeroDeReserved4", "PrimeroDeReserved5", "PrimeroDeReserved6", 
                     "PrimeroDeReserved7", "PrimeroDeReserved8", "PrimeroDeReserved9", "PrimeroDeReserved10", "Fecha")

colnames(data) = nombres_columnas

# Abro el historico
nombre_archivo_historico = "Compilado_facturas_tembici_historico.txt"

data_historico = fread(nombre_archivo_historico,
                       encoding = "UTF-8")

data$Fecha = as.character(data$Fecha)

data = rbind(data, data_historico)

columnas_conservar = c("Merchant Reference", "PrimeroDePayment Method", "PrimeroDePayment Method Variant", "PrimeroDeCreation Date", 
                       "PrimeroDeAmount", "PrimeroDeRisk Scoring", "PrimeroDeShopper Name", "PrimeroDeIssuer Name", 
                       "PrimeroDeIssuer Country", "PrimeroDeAcquirer Response", "PrimeroDeBIN Funding Source")



data = data %>% select(one_of(columnas_conservar))

nombres_columnas = c("ID_Trx", "Metodo_Pago","Metodo_Pago_Detalle", "Fecha_Creacion", "Monto", "Risk_Scoring", "Cliente", "Metodo_Pago_Emisor",
                     "Metodo_Pago_Emisor_Pais", "Respuesta", "Metodo_Pago_Tipo")

colnames(data) = nombres_columnas
data$Fecha_Creacion = ymd_hms(data$Fecha_Creacion)
data = unique(data)

# Ordeno los datos por ID_Trx y Fecha_Creacion
data = data[order(data$ID_Trx, data$Fecha_Creacion),]


# Copio el df desde la segunda fila, para en todos los casos quedarme con el registro siguiente
largo = nrow(data)
col_aux = data[2:largo, c(1,4,5,10)]
col_aux = add_row(col_aux)

colnames(col_aux) = c("ID_Trx_Sig", "Fecha_Creacion_Sig", "Monto_Sig", "Respuesta_Sig")
data = cbind(data, col_aux)

# Copio el df desde la segunda fila, para en todos los casos quedarme con el registro anterior
col_aux = data[1:largo-1, c(1,4,5,10)]
col_aux = add_row(col_aux, .before = 1)

colnames(col_aux) = c("ID_Trx_Ant", "Fecha_Creacion_Ant", "Monto_Ant", "Respuesta_Ant")
data = cbind(data, col_aux)

# Cambio ID = NA a Otra cosa
data$ID_Trx_Ant = ifelse(is.na(data$ID_Trx_Ant), "Poner_NA", data$ID_Trx_Ant)
data$ID_Trx_Sig = ifelse(is.na(data$ID_Trx_Sig), "Poner_NA", data$ID_Trx_Sig)


# Defino primera y última transacción si hay cambio de ID
data$Primera_Trx = ifelse(data$ID_Trx != data$ID_Trx_Ant, 1, 0)
data$Ultima_Trx = ifelse(data$ID_Trx != data$ID_Trx_Sig, 1, 0)

data$Trx_Saldada_En_El_Acto = ifelse(data$Primera_Trx == 1 & data$Ultima_Trx == 1 & data$Respuesta == 'APPROVED', 1, 0)
data$Trx_Abierta = ifelse(data$Ultima_Trx == 1 & data$Respuesta != 'APPROVED', 1, 0)
data$Trx_Cerrada = ifelse(data$Ultima_Trx == 1 & data$Respuesta == 'APPROVED', 1, 0)


# Armo dfs de primera y ultima transaccion
trx_saldadas = unique(data[which(data$Trx_Saldada_En_El_Acto == 1), 1])
trx_saldadas$Estado = 1

trx_cerradas = unique(data[which(data$Trx_Cerrada == 1), 1])
trx_cerradas$Estado = 1

df_primera = data[which(data$Primera_Trx == 1), 1:11]
nombres_columnas = c("ID_Trx_First", "Metodo_Pago_First","Metodo_Pago_Detalle_First", "Fecha_Creacion_First", "Monto_First", 
                     "Risk_Scoring_First", "Cliente_First", "Metodo_Pago_Emisor_First",
                     "Metodo_Pago_Emisor_Pais_First", "Respuesta_First", "Metodo_Pago_Tipo_First")

colnames(df_primera) = nombres_columnas

df_ultima = data[which(data$Ultima_Trx == 1), 1:11]
nombres_columnas = c("ID_Trx_Last", "Metodo_Pago_Last","Metodo_Pago_Detalle_Last", "Fecha_Creacion_Last", "Monto_Last", 
                     "Risk_Scoring_Last", "Cliente_Last", "Metodo_Pago_Emisor_Last",
                     "Metodo_Pago_Emisor_Pais_Last", "Respuesta_Last", "Metodo_Pago_Tipo_Last")

colnames(df_ultima) = nombres_columnas

# Voy armando el dataset de salida con las características de cada trx

data_out = as.data.frame(unique(data$ID_Trx))
colnames(data_out) = c("ID_Trx")

data_out = merge(data_out, trx_cerradas, by.x = "ID_Trx", by.y = "ID_Trx", all.x = TRUE)
data_out = merge(data_out, trx_saldadas, by.x = "ID_Trx", by.y = "ID_Trx", all.x = TRUE)

colnames(data_out) = c("ID_Trx", "Trx_Cerrada", "Trx_SaldadaEnElActo")
data_out$Trx_Cerrada = ifelse(is.na(data_out$Trx_Cerrada), 0, data_out$Trx_Cerrada)
data_out$Trx_SaldadaEnElActo = ifelse(is.na(data_out$Trx_SaldadaEnElActo), 0, data_out$Trx_SaldadaEnElActo)

data_out = merge(data_out, df_primera, by.x = "ID_Trx", by.y = "ID_Trx_First", all.x = TRUE)
data_out = merge(data_out, df_ultima, by.x = "ID_Trx", by.y = "ID_Trx_Last", all.x = TRUE)

data_out$Tiempo_Abierta = ifelse(data_out$Trx_Cerrada == 0,
                                 round((now() - data_out$Fecha_Creacion_First) * 86400,0),
                                 NA)
data_out$Tiempo_Resolucion = ifelse(data_out$Trx_Cerrada == 1,
                                    data_out$Fecha_Creacion_Last - data_out$Fecha_Creacion_First,
                                    NA)

# Guardo el archivo de salida
nombre_salida = "Historico_Facturas.txt"
directorio = "W:\\Ecobici\\Facturas\\"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(data_out, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t", dec = ",")



# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Descarga_Facturas"

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

