#------------------------------------------------------------------------------------------
# DB Paradas masivas
# 03/09/2021
# Acceso a la base de datos de paradas masivas
# ------------------------------------------------------------------------------------------

# IP: 10.22.0.158
# BD: tarifaszonas
# BD: paradasmasivas
# user: jlopez
# pass: jlopez2021
# 
# La BD está en PostgreSQL.

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Instalación condicional de paquetes

# Package names
packages <- c("dplyr", "jsonlite", "lubridate", "data.table", "googlesheets4", 
              "httr", "DBI", "RPostgreSQL", "naniar", "stringr")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))


db = 'tarifaszonas'
db = 'paradasmasivas'
host_db = '10.22.0.158'
db_port = '5432'
db_user = 'jlopez'
db_password = 'jlopez2021'

con = dbConnect(RPostgres::Postgres(), 
                dbname = db, 
                host = host_db, 
                port = db_port, 
                user = db_user, 
                password = db_password)

lista_tablas = 0
lista_tablas = dbListTables(con)
lista_tablas

ars_numeros = function(vector){
  vector = gsub("AR\\$", "", vector)
  vector = gsub(",", ".", vector)
  vector = as.numeric(vector)
}

guardar = function(df, nombre_df){
  directorio_out = "W:\\Paradas Masivas\\"
  file_out = paste(nombre_df, ".txt", sep = "")
  salida = paste(directorio_out, file_out, sep = "")
  fwrite(df, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")  
}


if (length(lista_tablas) > 1){
  
  conductores = dbReadTable(con, "conductores")
  #dispositivos = dbReadTable(con, "dispositivos")
  log_login_conductores = dbReadTable(con, "log_login_conductores")
  #log_estados_totem = dbReadTable(con, "logestadostotem")
  motivos = dbReadTable(con, "motivos")
  tickets = dbReadTable(con, "tickets")
  totems = dbReadTable(con, "totems")
  viajes_cancelados = dbReadTable(con, "viajes_cancelados")
  
  # Tabla viajes cancelados
  #-----------------------------------------------------------------------------------------------------
  viajes_cancelados = merge(viajes_cancelados, motivos, 
                            by.x = "motivoid",
                            by.y = "motivoid",
                            all.x = TRUE)
  
  viajes_cancelados$motivo_final = ifelse(viajes_cancelados$motivoid == 0,
                                          viajes_cancelados$motivo.x,
                                          viajes_cancelados$motivo.y)
  viajes_cancelados$motivo.x = NULL
  viajes_cancelados$motivo.y = NULL
  
  viajes_cancelados = viajes_cancelados %>% select(-contains("created"))
  viajes_cancelados = viajes_cancelados %>% select(-contains("modified"))
  
  rm("motivos")
  
  # Tabla viajes
  #-----------------------------------------------------------------------------------------------------
  na_strings <- c("NA", "N A", "N / A", "N/A", "N/ A", "Not Available", "NOt available", "")
  tickets = tickets %>% replace_with_na_all(condition = ~.x %in% na_strings)
  
  # Me parece mejor cambiar las columnas de paradas (con el detalle) por un simple contador de # paradas intermedias
  columnas = c("primera_parada", "segunda_parada", "tercera_parada")
  tickets$cantParadas = rowSums(!is.na(tickets[columnas]))
  
  tickets = tickets %>% select(-contains("_parada"))
  tickets = tickets %>% select(-contains("created"))
  tickets = tickets %>% select(-contains("modified"))
  
  # Remplazo valores de AR$
  tickets$costo_viaje_no_total = ars_numeros(tickets$costo_viaje_no_total)
  tickets$costo_retorno_caba = ars_numeros(tickets$costo_retorno_caba)
  tickets$adicional_por_equipaje = ars_numeros(tickets$adicional_por_equipaje)  
  tickets$bajada_de_bandera = ars_numeros(tickets$bajada_de_bandera)
  tickets$costo_viaje = ars_numeros(tickets$costo_viaje)
  
  # Me quedo con una sola columna de DNI y Dominio
  tickets$dniConductor = ifelse(is.na(tickets$dni_conductor),
                                tickets$dni_conductor_vencido,
                                tickets$dni_conductor)
  
  tickets$dominioConductor = ifelse(is.na(tickets$dominio_conductor),
                                    tickets$dominio_conductor_vencido,
                                    tickets$dominio_conductor)
  
  tickets = tickets %>% select(-contains("_conductor"))
  
  # Tabla login_conductores
  #-----------------------------------------------------------------------------------------------------
  log_login_conductores = log_login_conductores %>% select(-contains("created"))
  log_login_conductores = log_login_conductores %>% select(-contains("modified"))
  
  # Tabla totems
  #-----------------------------------------------------------------------------------------------------
  totems = totems %>% select(-contains("created"))
  totems = totems %>% select(-contains("modified"))
  
  # Tabla conductores
  #-----------------------------------------------------------------------------------------------------
  conductores = conductores %>% select(-contains("created"))
  conductores = conductores %>% select(-contains("modified"))
  
  
  
  # Guardo el archivo
  # lista de todos los dataframes
  x = sapply(sapply(ls(), get), is.data.frame)
  lista = names(x)[(x==TRUE)] 
  
  for (j in (1:length(lista))){
    df_name = lista[j]
    df = get(df_name)
    guardar(df, df_name)
  }
  
  # Escribo status ejecución en hoja "Status Datos SSGM"
  # --------------------------------------------------------------------------------
  gs4_auth(email = "datos.ssgm@gmail.com")
  id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
  df_hora_guardado = as.data.frame(Sys.time() - hours(3))
  colnames(df_hora_guardado) = "FechaHoraActual"
  
  codigo_r = "ConectaDB_Paradas_Masivas"
  
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
  
  dbDisconnect(con)  
}
