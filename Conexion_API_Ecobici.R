# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Instalación condicional de paquetes

# Package names
packages <- c("dplyr", "jsonlite", "lubridate", "data.table", "googlesheets4", "httr")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))


# https://www.buenosaires.gob.ar/desarrollourbano/transporte/apitransporte/api-doc
# /ecobici/gbfs/stationStatus
vclient_id='19117f54219f451aaebb050195f5e4cd'
vclient_secret= '6e969EA7E03D484dbf164f269F65C33B'

url='https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationStatus'

request = GET(
  url = url,
  query = list(client_id = vclient_id,
               client_secret = vclient_secret)
)

response = content(request, as = "text", encoding = "UTF-8")

df = fromJSON(response) %>% data.frame()

# Convierto los timestamp unix a datetime
df$last_updated = as.POSIXct(df$last_updated, origin="1970-01-01") - hours(3)
df$data.stations.last_reported = as.POSIXct(df$data.stations.last_reported, origin="1970-01-01")- hours(3)

# extraigo data.stations.num_bikes_avalable_types que es un dataframe
df2 = df$data.stations.num_bikes_available_types
colnames(df2) = c("num_bikes_available_mechanical", "num_bikes_available_ebike")

df$data.stations.num_bikes_available_types = NULL
df = cbind(df, df2)

df$bajada = now() - hours(3)

# Guardo el archivo
# Almaceno los últimos 6 meses de datos
fecha_corte = today() - months(6)

file_out = paste("stationStatus.txt", sep="")
directorio_out = "W:\\Ecobici\\API\\"
salida = paste(directorio_out, file_out, sep = "")

historico = fread(salida)
df$data.stations.station_id = as.integer(df$data.stations.station_id)
historico = dplyr :: bind_rows(historico, df)
historico = historico[which(historico$bajada > fecha_corte),]

fwrite(historico, 
       salida, 
       append = FALSE, 
       row.names = FALSE, 
       col.names = TRUE,
       #na = 0,
       sep = "\t")


# Ahora vamos con las estaciones
# /ecobici/gbfs/stationInformation

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

vclient_id='19117f54219f451aaebb050195f5e4cd'
vclient_secret= '6e969EA7E03D484dbf164f269F65C33B'

url='https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationInformation'

request = GET(
  url = url,
  query = list(client_id = vclient_id,
               client_secret = vclient_secret)
)

response = content(request, as = "text", encoding = "UTF-8")

df = fromJSON(response) %>% data.frame()

df$data.stations.physical_configuration = NULL
df$data.stations.altitude = NULL
df$data.stations.post_code = NULL
df$data.stations.rental_methods = NULL
df$data.stations.obcn = NULL
df$data.stations.nearby_distance = NULL
df$data.stations.cross_street = NULL

df2 = t(as.data.frame(df$data.stations.groups))
df2 = as.data.frame(df2[,1])
colnames(df2) = c("data.stations.groups")

df$data.stations.groups = NULL
df = cbind(df, df2)

df$last_updated = as.POSIXct(df$last_updated, origin="1970-01-01") - hours(3)
df$bajada = now() - hours(3)

# Guardo el archivo
file_out = paste("stationInformation.txt", sep="")
directorio_out = "W:\\Ecobici\\API\\"
salida = paste(directorio_out, file_out, sep = "")
historico = fread(salida)
df$data.stations.station_id = as.integer(df$data.stations.station_id)

historico = dplyr :: bind_rows(historico, df)
historico = historico %>% distinct(data.stations.station_id,
                                   data.stations.name,
                                   data.stations.lat,
                                   data.stations.lon,
                                   data.stations.address,
                                   data.stations.capacity,
                                   data.stations.groups, 
                                   .keep_all = TRUE)

fwrite(historico, 
       salida, 
       append = FALSE, 
       row.names = FALSE, 
       col.names = TRUE,
       #na = 0,
       sep = "\t")

# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Conexion_API_Ecobici"

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
