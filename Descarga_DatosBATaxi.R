#------------------------------------------------------------------------------------------
# DB BA Taxi
# 20/08/2021
# Acceso a las bases de datos BATaxi y SACTA (VHT - Verificación y Habilitación de Transporte (Ex Sacta))
#------------------------------------------------------------------------------------------

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Instalación condicional de paquetes

# Package names
packages <- c("lubridate", "data.table", "dplyr", "dbplyr", "odbc", "DBI", "RMySQL", "googlesheets4")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

# DB BATAXI

# Hostname: dbm10n4.gcba.gob.ar
# Puerto: 3306
# Nombre DB: bataxi

db = 'bataxi'
host_db = 'dbm10n4.gcba.gob.ar'
db_port = 3306
db_user = 'jlopezsaez'
db_password = 'GOb21x_JL'
conexion = 'BATaxi'
# db_user = 'mareso'
# db_password = 'Mare_g21xx'

con = dbConnect(MySQL(),
                 user='jlopezsaez',
                 password='GOb21x_JL',
                 dbname='bataxi',
                 host='dbm10n4.gcba.gob.ar')

lista_tablas = 0
lista_tablas = dbListTables(con)

guardar = function(df, nombre_df, directorio){
  directorio_out = directorio
  file_out = paste(nombre_df, ".txt", sep = "")
  salida = paste(directorio_out, file_out, sep = "")
  fwrite(df, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")  
}

if (length(lista_tablas) > 1){
  
  # tablas que por ahora veo que me interesan
  tablas = c('cars', 'cars_users', 'claims', 'payments', 'ratings', 'tag_categories', 
             'tags', 'tags_taxi_bookings', 'tags_users', 'taxi_bookings', 'tickets', 'users')
  
  for (tabla in tablas){
    assign(tabla, dbReadTable(con, tabla))
  }
  
  # Guardo el archivo
  # lista de todos los dataframes
  x = sapply(sapply(ls(), get), is.data.frame)
  lista = names(x)[(x==TRUE)] 
  
  directorio = "W:\\BA Taxi\\DB_BATaxi\\"
  for (j in (1:length(lista))){
    df_name = lista[j]
    df = get(df_name)
    guardar(df, df_name, directorio)
  }
  dbDisconnect(con)  
}

# --------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------
# Limpio la memoria
rm(list=ls())
gc()

# DB SACTA
# Hostname: sqlserver-bataxi.gcba.gob.ar
# Puerto: 1433
# Nombre DB: sacta

db = 'SACTA'
host_db = 'sqlserver-bataxi.gcba.gob.ar'
db_port = 1433
db_user = 'jlopezsaez'
db_password = 'GOb21x_JL'
conexion = 'sacta'
#db_user = 'mareso'
#db_password = 'Mare_g21xx'

# con = DBI::dbConnect(odbc::odbc(),
#                      connect = conexion,
#                      Driver = "SQL Server Native Client 11.0",
#                      Server = host_db,
#                      Database = db,
#                      uid = db_user,
#                      pwd = db_password,
#                      port = db_port)
# 
# lista_tablas = 0
# lista_tablas = dbListTables(con)





# --------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------
# Limpio la memoria
rm(list=ls())
gc()

# Hostname: sqlserver-bataxi.gcba.gob.ar
# Puerto: 1433
# Nombre DB: TaxisService

db = 'TaxisService'
host_db = 'sqlserver-bataxi.gcba.gob.ar'
db_port = 1433
db_user = 'jlopezsaez'
db_password = 'GOb21x_JL'
conexion = 'TaxisService'
#db_user = 'mareso'
#db_password = 'Mare_g21xx'

con = DBI::dbConnect(odbc::odbc(),
                     connect = conexion,
                     Driver = "SQL Server Native Client 11.0",
                     Server = host_db,
                     Database = db,
                     uid = db_user,
                     pwd = db_password,
                     port = db_port)

lista_tablas = 0
lista_tablas = dbListTables(con)

guardar = function(df, nombre_df, directorio){
  directorio_out = directorio
  file_out = paste(nombre_df, ".txt", sep = "")
  salida = paste(directorio_out, file_out, sep = "")
  fwrite(df, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")  
}

if (length(lista_tablas) > 1){
  # tablas que por ahora veo que me interesan
  tablas = c('Conductores', 'ConductoresConductores', 'ConductoresEstados', 'ConductoresLogin', 'ConductoresTipos', 
             'ConductoresVehiculos', 'DominiosAEP', 'ExtensionesConductoresConductores', 
             'ExtensionesConductoresConductoresLog', 'Mandatarias', 'MandatariasConductores', 
             'MandatariasEstados', 'MandatariasVehiculos', 'PagosTipos', 'Usuarios', 'UsuariosEstados', 
             'Vehiculos', 'VehiculosEstados', 'Viajes')
 
  for (tabla in tablas){
    assign(tabla, dbReadTable(con, tabla))
  }
  
  # Guardo el archivo
  # lista de todos los dataframes
  x = sapply(sapply(ls(), get), is.data.frame)
  lista = names(x)[(x==TRUE)] 
  
  directorio = "W:\\BA Taxi\\DB_TaxisService\\"
  for (j in (1:length(lista))){
    df_name = lista[j]
    df = get(df_name)
    guardar(df, df_name, directorio)
  }
  
  dbDisconnect(con)  
}


# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "ConectaDB_BATaxi"

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


