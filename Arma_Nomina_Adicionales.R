# 23/08/2021
# ----------------------------------------------------------------------------------------------------------------
# JIR quiere una salida de nómina CAT en donde figuren, ademas, valores promedio de presentismo por agente por año
# Tomamos el archivo "Data_Presentismo.txt" y lo procesamos para realizar dicho cálculo
# En este archivo, junto la bajada de nomina, con los numeros CAT y los datos de presentismo.
# ----------------------------------------------------------------------------------------------------------------

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Instalación condicional de paquetes

# Package names
packages <- c("dplyr", "lubridate", "data.table", "readxl", "googlesheets4")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

# Abro el archivo
file = "Data_Presentismo_3yr.txt"
directorio = "W:\\Agentes Viales\\Nomina\\Bajada_Nomina_Adicional\\"
file = paste(directorio, file, sep = "")
data_presentismo = fread(file,
                         encoding = 'UTF-8')

file = "Nomina_CAT_Adicional.txt"
directorio = "W:\\Agentes Viales\\Nomina\\Bajada_Nomina_Adicional\\"
file = paste(directorio, file, sep = "")
data_nomina = fread(file,
                    encoding = 'UTF-8')

# 20210825 Ahora ya tengo los nros_cat desde la DB de nómina, y está como campo en data_nomina
# 
# file = "DGCATRA-Listado Nro CAT.xlsx"
# directorio = "W:\\Agentes Viales\\Nomina\\"
# file = paste(directorio, file, sep = "")
# 
# nros_cat_1 = read_xlsx(
#   file,
#   sheet = "CAT ACTIVOS",
#   col_names = TRUE,
#   .name_repair = "unique")
# 
# nros_cat_2 = read_xlsx(
#   file,
#   sheet = "CAT BAJAS",
#   col_names = TRUE,
#   .name_repair = "unique")
# 
# nros_cat = rbind(nros_cat_1, nros_cat_2)

# Elimino duplicados
data_nomina = data_nomina %>% distinct(CUIT, .keep_all = TRUE)
data_presentismo = data_presentismo %>% distinct(cuit, .keep_all = TRUE)
#nros_cat = nros_cat %>% distinct(CUIT, .keep_all = TRUE)

# Elimino columnas adicionales
data_presentismo$id_agente = NULL
# nros_cat$APELLIDO = NULL
# nros_cat$NOMBRE = NULL
# nros_cat$DNI = NULL
# nros_cat$CARGO = NULL
# nros_cat$Version = NULL
# nros_cat$OBSERVACIONES = NULL

# Hago los merge con nomina
#data_nomina = merge(data_nomina, nros_cat, by.x = "CUIT", by.y = "CUIT", all.x = TRUE)
data_nomina = merge(data_nomina, data_presentismo, by.x = "CUIT", by.y = "cuit", all.x = TRUE)

# Guardo el archivo de salida
file_out = "Nomina_CAT_Adicionales.txt"
directorio_out = "W:\\Agentes Viales\\Nomina\\"
salida = paste(directorio_out, file_out, sep = "")

fwrite(data_nomina, 
       salida, 
       append = FALSE, 
       row.names = FALSE, 
       col.names = TRUE,
       #na = 0,
       sep = "\t")

# Lo guardo en un google sheets también
gs4_auth(email = "datos.ssgm@gmail.com")
df_hora_guardado = as.data.frame(Sys.time() - hours(3))

id_sheets = "1rBKpw_G_D0F27sQAVEOi9U4AphFsEwwSPicRGPJB0Zk"
celda = "A1"
datos = as.data.table(data_nomina)
datos$`Telefono casa` = as.character(datos$`Telefono casa`)
datos$`Telefono ht` = as.character(datos$`Telefono ht`)

tryCatch({
  range_write(
    ss = id_sheets,
    data = datos,
    sheet = 'Nomina',
    range = celda,
    col_names = TRUE,
    reformat = FALSE
  )
}, error=function(e){})

# Guardo el dato de ultimo update
celda = "B1"

tryCatch({
  range_write(
    ss = id_sheets,
    data = df_hora_guardado,
    sheet = 'Ultima Actualizacion',
    range = celda,
    col_names = FALSE,
    reformat = FALSE
  )
}, error=function(e){})

# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------

id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"

colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Genera Nomina con Adicionales"

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


