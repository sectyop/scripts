# Procesar la descarga vieja de Infracciones para adecuar a la nueva base

# columnas_nombres = colnames(db_nueva)
# db_final = data.frame(matrix(ncol = length(columnas_nombres), nrow = 0))
# colnames(db_final) = columnas_nombres
# rm("db_final")
# names(db_vieja)[names(db_vieja) == 'ActaID'] = 'ID'
# names(db_vieja)[names(db_vieja) == 'FechaCaptura'] = 'FECH'
# names(db_vieja)[names(db_vieja) == 'Calle'] = 'CALL'
# names(db_vieja)[names(db_vieja) == 'Altura'] = 'ALTU'
# names(db_vieja)[names(db_vieja) == 'DispositivoID'] = 'IMEI'
#db_final = rbind(db_nueva, db_vieja_seleccion)

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)
library(readxl)
library(googlesheets4)

directorio_vieja = "W:\\Agentes Viales\\Infracciones\\Vieja_DB\\"
directorio_nueva = "W:\\Agentes Viales\\Infracciones\\Nueva_DB\\"

directorio_out = "W:\\Agentes Viales\\Infracciones\\Nueva_DB\\Vieja_DB_NuevoFormato\\"

file = paste(directorio_vieja, "Actas.txt", sep = "")
db_vieja = fread(file, 
                 header = TRUE, 
                 sep = "\t", 
                 encoding = 'UTF-8',
                 stringsAsFactors = TRUE)
db_vieja$ActaID = paste('Vieja_DB_', db_vieja$ActaID, sep = '')

print("....................................1")
# 1) Armar acta_infraccion.txt
file = paste(directorio_nueva, "acta_infraccion.txt", sep = "")
db_nueva = fread(file, 
                 header = TRUE, 
                 sep = "\t", 
                 encoding = 'UTF-8',
                 stringsAsFactors = TRUE)
db_nueva$ID = as.character(db_nueva$ID)

agentes = fread("W:\\Agentes Viales\\Infracciones\\Vieja_DB\\Actas_Numeros.txt",
                header = TRUE,
                sep = "\t",
                encoding = "UTF-8")
agentes$Id = paste('Vieja_DB_', agentes$Id, sep = '')

ref_agentes = fread("W:\\Agentes Viales\\Infracciones\\Nueva_DB\\Vieja_DB_NuevoFormato\\ref_id_agentes.txt",
                    header = TRUE,
                    sep = "\t",
                    encoding = "UTF-8")

db_vieja_seleccion = db_vieja %>% select(ActaID, FechaCaptura, Calle, Altura, DispositivoID)
db_vieja_seleccion = merge(db_vieja_seleccion, agentes, by.x = "ActaID", by.y = "Id", all.x = TRUE)
db_vieja_seleccion = merge(db_vieja_seleccion, ref_agentes, by.x = "AgenteID", by.y = "AgenteID", all.x = TRUE)

db_vieja_seleccion$AgenteID = NULL
db_vieja_seleccion$Numero = NULL
db_vieja_seleccion$LogDateCreated = NULL

colnames(db_vieja_seleccion) = c('ID', 'FECH', 'CALL', 'ALTU', 'IMEI', 'CUENTA_FK')

db_nueva$IMEI = as.numeric(db_nueva$IMEI)
db_vieja$DispositivoID = as.numeric(db_vieja$DispositivoID)
db_final = dplyr::bind_rows(db_nueva, db_vieja_seleccion)

# Guardo el archivo
file_out = "acta_infraccion.txt"
salida = paste(directorio_out, file_out, sep = "")
fwrite(db_final, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

rm("ref_agentes", "agentes")

print("....................................2")
# 2) Armar acta_infraccion_infraccion.txt
file = paste(directorio_nueva, "acta_infraccion_infraccion.txt", sep = "")
db_nueva = fread(file, 
                 header = TRUE, 
                 sep = "\t", 
                 encoding = 'UTF-8',
                 stringsAsFactors = TRUE)
db_nueva$ACTA_INFRACCION_FK = as.character(db_nueva$ACTA_INFRACCION_FK)

db_vieja_seleccion = db_vieja %>% select(ActaID, CodigoInfraccion)
colnames(db_vieja_seleccion) = c('ACTA_INFRACCION_FK', 'TIPO_INFRACCIONES_FK')

db_final = dplyr::bind_rows(db_nueva, db_vieja_seleccion)

# Guardo el archivo
file_out = "acta_infraccion_infraccion.txt"
salida = paste(directorio_out, file_out, sep = "")
fwrite(db_final, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

print("....................................3")
# 3) Armar acta_infraccion_vehiculo.txt
file = paste(directorio_nueva, "acta_infraccion_vehiculo.txt", sep = "")
db_nueva = fread(file, 
                 header = TRUE, 
                 sep = "\t", 
                 encoding = 'UTF-8',
                 stringsAsFactors = TRUE)
db_nueva$ACTA_INFRACCION_FK = as.character(db_nueva$ACTA_INFRACCION_FK)

db_vieja_seleccion = db_vieja %>% select(ActaID, Dominio, TipoVehiculo, Marca, Modelo, VehiculoRemision)

playas = fread("W:\\Agentes Viales\\Infracciones\\Nueva_DB\\Vieja_DB_NuevoFormato\\ref_playas_id.txt",
               header = TRUE,
               sep = "\t",
               encoding = "UTF-8")

path_ = "W:\\Agentes Viales\\Infracciones\\Ref_Vehiculos_Remitidos.xlsx"
ref_vehiculos_remitidos = read_xlsx(path = path_,
                                    .name_repair = "unique",
                                    sheet = "Ref")  

db_vieja_seleccion = merge(db_vieja_seleccion, ref_vehiculos_remitidos, by.x = "VehiculoRemision", by.y = "Nombre", all.x = TRUE)
db_vieja_seleccion = merge(db_vieja_seleccion, playas, by.x = "Base", by.y = "Base", all.x = TRUE)

db_vieja_seleccion$Base = NULL
db_vieja_seleccion$VehiculoRemision = NULL
db_vieja_seleccion$Cantidad = NULL
db_vieja_seleccion$Nombre_Corregido = NULL

colnames(db_vieja_seleccion) = c('ACTA_INFRACCION_FK', 'NDOM', 'TIPO_VEHICULO_FK', 'Marca', 'Modelo', 'PLAYAS_FK')

modelos = fread("W:\\Agentes Viales\\Infracciones\\Nueva_DB\\Vieja_DB_NuevoFormato\\ref_modelo_vehiculo.txt",
                header = TRUE,
                sep = "\t",
                encoding = "UTF-8")
marcas = fread("W:\\Agentes Viales\\Infracciones\\Nueva_DB\\Vieja_DB_NuevoFormato\\ref_marca_vehiculos.txt",
               header = TRUE,
               sep = "\t",
               encoding = "UTF-8")

db_vieja_seleccion = merge(db_vieja_seleccion, modelos, by.x = "Modelo", by.y = "Modelo", all.x = TRUE)
db_vieja_seleccion = merge(db_vieja_seleccion, marcas, by.x = "Marca", by.y = "Marca", all.x = TRUE)

names(db_vieja_seleccion)[names(db_vieja_seleccion) == 'ID_Modelo'] = 'MODELO_VEHICULO_FK'
names(db_vieja_seleccion)[names(db_vieja_seleccion) == 'ID_Marca'] = 'MARCA_VEHICULO_FK'
db_vieja_seleccion$Marca = NULL
db_vieja_seleccion$Modelo = NULL
rm("marcas", "modelos", "playas", "ref_vehiculos_remitidos")

db_final = dplyr::bind_rows(db_nueva, db_vieja_seleccion)

# Guardo el archivo
file_out = "acta_infraccion_vehiculo.txt"
salida = paste(directorio_out, file_out, sep = "")
fwrite(db_final, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


# 4) Armar acta_infraccion_transporte_publico.txt
# file = paste(directorio_nueva, "acta_infraccion_transporte_publico.txt", sep = "")
# db_nueva = fread(file, 
#                  header = TRUE, 
#                  sep = "\t", 
#                  encoding = 'UTF-8',
#                  stringsAsFactors = TRUE)
# 
# db_vieja_seleccion = db_vieja %>% select(ActaID, ColectivoEmpresa, ColectivoLinea, ColectivoInterno)
# # 
# 
# colnames(db_vieja_seleccion) = c('ACTA_INFRACCION_FK', 'NDOM', 'TIPO_VEHICULO_FK', 'Marca', 'Modelo')
# 

# db_final = dplyr::bind_rows(db_nueva, db_vieja_seleccion)
# 
# # Guardo el archivo
# file_out = "acta_infraccion_transporte_publico.txt"
# salida = paste(directorio_out, file_out, sep = "")
# fwrite(db_final, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

print("....................................5")
# 5) Armo base unificada de direcciones geoloc
file = paste(directorio_nueva, "acta_infraccion.txt", sep = "")
db_nueva = fread(file, 
                 header = TRUE, 
                 sep = "\t", 
                 encoding = 'UTF-8',
                 stringsAsFactors = TRUE)
db_nueva$ID = as.character(db_nueva$ID)

direcciones_nuevadb = db_nueva %>% select(CALL, ALTU, LAT, LNG)
direcciones_nuevadb = unique(direcciones_nuevadb)

direcciones_nuevadb$Altura = round(direcciones_nuevadb$ALTU / 100 , 0) * 100
direcciones_nuevadb = direcciones_nuevadb[which(!is.na(direcciones_nuevadb$Altura)),]
direcciones_nuevadb$Calle = as.character(direcciones_nuevadb$CALL)
direcciones_nuevadb$lat = as.character(direcciones_nuevadb$LAT)
direcciones_nuevadb$lon = as.character(direcciones_nuevadb$LNG)
direcciones_nuevadb$Corregida = toupper(paste(direcciones_nuevadb$Calle, direcciones_nuevadb$Altura, sep = " "))

direcciones_nuevadb$CALL = NULL
direcciones_nuevadb$ALTU = NULL
direcciones_nuevadb$LAT = NULL
direcciones_nuevadb$LNG = NULL

ref_geoloc = fread("W:\\Agentes Viales\\Infracciones\\Vieja_DB\\Ref_Direcciones_LatLon.txt",
                   header = TRUE,
                   sep = "\t",
                   encoding = "UTF-8")

db_final = dplyr::bind_rows(ref_geoloc, direcciones_nuevadb)
db_final = db_final[order(db_final$Calle, db_final$Altura),]
db_final = db_final[!duplicated(db_final[,c('Calle','Altura')]),]

# Guardo el archivo
file_out = "ref_geolocalizacion_calles.txt"
salida = paste(directorio_out, file_out, sep = "")
fwrite(db_final, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Consolida_Bases"

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

