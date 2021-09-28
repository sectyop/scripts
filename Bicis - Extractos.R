# 19/2/2020
# LO QUE QUIERO TRATAR ES DE OBTENER UN DF CON LOS ULTIMOS DATOS DEL USO DE CADA BICI
# Y VER DE TENER OTRO CON EL STATUS DE CADA BICI: ACTIVA O ROBADA, POR FECHA

#data = acumulado
# 1) OBTENGO ULTIMO VIAJE DE CADA BICICLETA
# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)
library(googlesheets4)

directorio = "w:\\Ecobici\\"
setwd(directorio)

file = "Bicicletas_acumulado_procesado_historico.txt"

#numero_estimado_filas = 7000000
#clases = c("character", "character", "factor", "character", "factor", "character", "factor", "character", 
#           "character", "character", "character", "character", "character")

Sys.time()
data = fread(file, 
             header = TRUE, 
             sep = "\t", 
             #colClasses = clases,
             #nrows = numero_estimado_filas,
             stringsAsFactors = TRUE)
Sys.time()

seleccion_columnas = c("ID_Viaje", "Msnbc_de_bicicleta", 
                       "Fecha_Inicio", "Fecha_Final", "ID_de_ciclista", "Nombre_de_ciclista", "Apellido_de_ciclista",
                       "Nombre_Inicio_Viaje", "LAT_Inicio_Viaje", "LON_Inicio_Viaje", "Barrio_Inicio_Viaje",
                       "Comuna_Inicio_Viaje", "Nombre_Final_Viaje", "LAT_Final_Viaje", "LON_Final_Viaje", 
                       "Barrio_Final_Viaje", "Comuna_Final_Viaje")

bicis = data %>% select(seleccion_columnas)
# bicis = data.frame(data[, c("ID_Viaje", "Numero_Inicio_Viaje", "Numero_Final_Viaje", "Msnbc_de_bicicleta", 
#                               "Fecha_Inicio", "Fecha_Final", "ID_de_ciclista", "Nombre_de_ciclista", "Apellido_de_ciclista",
#                               "Nombre_Inicio_Viaje", "LAT_Inicio_Viaje", "LON_Inicio_Viaje", "Barrio_Inicio_Viaje",
#                               "Comuna_Inicio_Viaje", "Nombre_Final_Viaje", "LAT_Final_Viaje", "LON_Final_Viaje", 
#                               "Barrio_Final_Viaje", "Comuna_Final_Viaje", "Año_Inicio", "Mes_Inicio", "Dia_Inicio")])

bicis$Fecha = as.Date(bicis$Fecha_Inicio)

a=nrow(bicis)
bicis = bicis[!(is.na(bicis$Msnbc_de_bicicleta) | bicis$Msnbc_de_bicicleta == ""), ]
bicis = bicis[!(bicis$Msnbc_de_bicicleta == "\017"), ]
b=nrow(bicis)
b/a

bicis$Msnbc_de_bicicleta = as.character(bicis$Msnbc_de_bicicleta)

bicis = as.data.table(bicis)
bicis = setorder(bicis, "Msnbc_de_bicicleta", "Fecha_Inicio")
#bicis = bicis[order(bicis[,2], bicis[,3]),]

col_aux = bicis[2:nrow(bicis), 2]
col_aux = add_row(col_aux, "Msnbc_de_bicicleta" = "xxx")

names(col_aux) = c("ID_Bici_Auxiliar")
bicis = cbind(bicis, col_aux)

ultima = rep(FALSE, nrow(bicis))
ultima = ifelse(as.character(bicis$Msnbc_de_bicicleta) == as.character(bicis$ID_Bici_Auxiliar), FALSE, TRUE)
bicis = cbind(bicis, ultima)

col_aux_2 = bicis[1:nrow(bicis)-1, 2]
col_aux_3 = data.frame(c("xxx"))
colnames(col_aux_3) = c("Msnbc_de_bicicleta")
col_aux_2 = rbind(col_aux_3, col_aux_2)
colnames(col_aux_2) = c("ID_Bici_Auxiliar_2")

bicis = cbind(bicis, col_aux_2)

rm("col_aux", "col_aux_2", "col_aux_3")
primera = (rep(FALSE, nrow(bicis)))
primera = ifelse((as.character(bicis$Msnbc_de_bicicleta) == as.character(bicis$ID_Bici_Auxiliar_2)), FALSE, TRUE)
#as.character(bicis$Msnbc_de_bicicleta)

bicis = cbind(bicis, primera)

bicis_ultima = bicis[which(bicis$ultima == TRUE)]
bicis_primera = bicis[which(bicis$primera == TRUE)]

primera_fecha = bicis_primera[,c("Msnbc_de_bicicleta", "Fecha")]
ultima_fecha = bicis_ultima[,c("Msnbc_de_bicicleta", "Fecha")]

# Este es el dataset con el ultimo viaje de cada bici.

# .-.-.-.-.- Genero el dataset de salida sólo con las columnas que me interesan.-.-.-.-.-
ultima_bicis = "Ultimo_Viaje_Bicicleta.txt"
primera_bicis = "Primer_Viaje_Bicicleta.txt"
directorio_out = directorio

ultima_bicis = paste(directorio_out, ultima_bicis, sep = "")
primera_bicis = paste(directorio_out, primera_bicis, sep = "")

fwrite(bicis_ultima, ultima_bicis, append = FALSE, row.names = FALSE, col.names = TRUE)
fwrite(bicis_primera, primera_bicis, append = FALSE, row.names = FALSE, col.names = TRUE)

# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------

# 2) OBTENGO EL HISTORIAL DE ESTADO DE CADA BICICLETA
rm(list=ls()[! ls() %in% c("data", "primera_fecha", "ultima_fecha")])
gc()

directorio = "w:\\Ecobici\\"
setwd(directorio)

data2 = data.frame(data[, c("Fecha_Inicio", "Msnbc_de_bicicleta")])

rm("data")
gc()

data2$Fecha_Inicio = as.Date(data2$Fecha_Inicio)

data2 = data2 %>%
  group_by(Fecha_Inicio, Msnbc_de_bicicleta) %>%
  summarise(count=n())

data2$Fecha = as.Date(data2$Fecha_Inicio)
data2 = as.data.frame(data2)

data2_fechas = as.data.frame(unique(data2$Fecha))
#data2_fechas$Fecha = ISOdate(data2_fechas$Año_Inicio, data2_fechas$Mes_Inicio, data2_fechas$Dia_Inicio)

data2_bicis = data2 %>%
  group_by(Msnbc_de_bicicleta) %>%
  summarise(count=n())

data2_bicis = data2_bicis[!(is.na(data2_bicis$Msnbc_de_bicicleta) | data2_bicis$Msnbc_de_bicicleta == ""), ]
data2_bicis = data2_bicis[!(data2_bicis$Msnbc_de_bicicleta == "\017"), ]

data2_bicis = merge(data2_bicis, ultima_fecha, by.x = "Msnbc_de_bicicleta", by.y = "Msnbc_de_bicicleta", all.x = TRUE)
colnames(data2_bicis) = c("Msnbc_de_bicicleta", "Viajes", "Ultima_Fecha")

data2_bicis = merge(data2_bicis, primera_fecha, by.x = "Msnbc_de_bicicleta", by.y = "Msnbc_de_bicicleta", all.x = TRUE)
colnames(data2_bicis) = c("Msnbc_de_bicicleta", "Viajes", "Ultima_Fecha", "Primera_Fecha")


filas_out = nrow(data2_fechas) * nrow(data2_bicis)
A = data.frame(rep(NA, filas_out))
B = data.frame(rep(NA, filas_out))
C = data.frame(rep(0, filas_out))
estado_out = cbind(A, B, C)
rm("A", "B", "C")

names(estado_out) = c("Msnbc_Bicicleta", "Fecha", "Estado")

fechas = do.call("rbind", replicate(nrow(data2_bicis), data2_fechas, simplify = FALSE))
estado_out$Fecha = fechas$`unique(data2$Fecha)`
rm("fechas")

estado_out$Msnbc_Bicicleta = rep(data2_bicis$Msnbc_de_bicicleta, each = nrow(data2_fechas))
estado_out$Ultima_fecha = rep(data2_bicis$Ultima_Fecha, each = nrow(data2_fechas))
estado_out$Primera_fecha = rep(data2_bicis$Primera_Fecha, each = nrow(data2_fechas))

estado_out$Estado = ifelse((estado_out$Fecha <= estado_out$Ultima_fecha) & 
                             (estado_out$Fecha >= estado_out$Primera_fecha), 
                           1, 
                           0)

estado_out$Ultima_fecha = NULL
estado_out$Primera_fecha = NULL

# .-.-.-.-.- Genero el dataset de salida sólo con las columnas que me interesan.-.-.-.-.-
salida_bicis = "Sistema_Bicis.txt"
salida_bicis_2 = "Bici_Primera_Ultima_Fecha.txt"

directorio_out = directorio
salida_bicis = paste(directorio_out, salida_bicis, sep = "")

fwrite(estado_out, salida_bicis, append = FALSE, row.names = FALSE, col.names = TRUE)
fwrite(data2_bicis, salida_bicis_2, append = FALSE, row.names = FALSE, col.names = TRUE)





# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Bicis - Extractos"

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


