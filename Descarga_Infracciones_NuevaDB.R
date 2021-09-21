#------------------------------------------------------------------------------------------
# Infracciones agentes
# 17/03/2021
# Acceso a la base de datos de infracciones
#------------------------------------------------------------------------------------------

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)
library(odbc)
library(DBI)
library(stringr)
library(googlesheets4)


db = 'DGISPR12'
host_db = 'dgispr12-scan.gcba.gob.ar'
db_port = 1521
db_user = 'jlopez'
db_password = 'N2RP6Kby'
coneccion = 'infracciones_oracle'


con = DBI::dbConnect(odbc::odbc(),
                     coneccion,
                     Driver = "oracledb",
                     Server = host_db,
                     Database = db,
                     uid = db_user,
                     pwd = db_password,
                     port = db_port)

lista_tablas = 0
lista_tablas = as.data.frame(dbListTables(con))

guardar = function(df, nombre_df){
  directorio_out = "W:\\Agentes Viales\\Infracciones\\Nueva_DB\\"
  file_out = paste(nombre_df, ".txt", sep = "")
  salida = paste(directorio_out, file_out, sep = "")
  fwrite(df, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")  
}


if (nrow(lista_tablas) > 1){
  
  acta_infraccion = dbGetQuery(con,'SELECT * FROM AGENTES.ACTA_INFRACCION')
  acta_infraccion_imputado = dbGetQuery(con,'SELECT * FROM AGENTES.ACTA_INFRACCION_IMPUTADO')
  acta_infraccion_infraccion = dbGetQuery(con,'SELECT * FROM AGENTES.ACTA_INFRACCION_INFRACCION')
  acta_infraccion_transporte_publico = dbGetQuery(con,'SELECT * FROM AGENTES.ACTA_INFRACCION_TRANSPORTE_PUBLICO')
  acta_infraccion_vehiculo = dbGetQuery(con,'SELECT * FROM AGENTES.ACTA_INFRACCION_VEHICULO')
  dgai = dbGetQuery(con,'SELECT * FROM AGENTES.DGAI')
  
  ref_sexos = dbGetQuery(con,'SELECT * FROM AGENTES.SEXOS')
  ref_tipo_documento = dbGetQuery(con,'SELECT * FROM AGENTES.TIPO_DOCUMENTOS')
  ref_tipo_dominions = dbGetQuery(con,'SELECT * FROM AGENTES.TIPO_DOMINIOS')
  ref_tipo_infracciones = dbGetQuery(con,'SELECT * FROM AGENTES.TIPO_INFRACCIONES')
  ref_tipo_vehiculos = dbGetQuery(con,'SELECT * FROM AGENTES.TIPO_VEHICULOS')
  ref_marcas_vehiculos = dbGetQuery(con,'SELECT * FROM AGENTES.MARCA_VEHICULOS')
  ref_modelo_vehiculos = dbGetQuery(con,'SELECT * FROM AGENTES.MODELO_VEHICULOS')
  ref_empresa_colectivos = dbGetQuery(con,'SELECT * FROM AGENTES.EMPRESA_COLECTIVOS')
  ref_lineas_colectivos = dbGetQuery(con,'SELECT * FROM AGENTES.LINEAS_COLECTIVOS')
  ref_categoria_licencia = dbGetQuery(con,'SELECT * FROM AGENTES.CATEGORIA_LICENCIAS')
  ref_cuentas_agentes = dbGetQuery(con,'SELECT * FROM AGENTES.CUENTAS')
  ref_playas = dbGetQuery(con,'SELECT * FROM AGENTES.PLAYAS')
  
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
  
  codigo_r = "Descarga_Infracciones"
  
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




