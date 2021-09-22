# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Instalación condicional de paquetes

# Package names
packages <- c("dplyr", "lubridate", "data.table", "googledrive", "readxl", "gtools", 
              "googlesheets4", "naniar", "stringr", "writexl")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

drive_auth(email = "datos.ssgm@gmail.com")

# Descargo los archivo CargaDatos de las 3 playas
id_dakota = "1uWRK61gNd4l_ZXIjO0nS61dtvu2x8PQyfvCP6DdSXJA"
id_tacuari = "18TKp4mqriQePxRn7galO-xsea9eo09-FySebaIgOSzo"
id_sarmiento = "1jP8_Eg_tVoHVxPu_iuNUOP6Ivmd7rUo8QFzS3dszG5M"

id_archivos = c(id_dakota, id_tacuari, id_sarmiento)
nombres_archivos = c("CargaDatos_Dakota.xlsx", "CargaDatos_Tacuari.xlsx", "CargaDatos_Sarmiento.xlsx")
base = NULL
carga_datos = NULL
detalles = NULL
columnas = c('FECHA', 'AGENTE', 'GRUA', 'DOMINIO', 'MARCA', 'MODELO', 'INFRAC', 'Nº DE ACTA', 'CALLE ACARREO', 
             'ALTURA', 'HORA INGRESO', 'DOLLY', 'PAPEL', 'LICENCIA', 'T/A', 'HORA EGRESO', 'TALON', 'Base')

for (k in (1:length(id_archivos))){
  # k = 1
  # base[[2]]
  # archivo = as_id(id_sarmiento)
  archivo = as_id(id_archivos[k])
  nombre_archivo = nombres_archivos[k]
  
  # Descargo el archivo
  directorio = "W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\Archivos\\"
  path_ = paste(directorio, nombre_archivo, sep = "")
  
  drive_download(
    file = archivo,
    path = path_,
    overwrite = TRUE)
  
  # Guardo una copia, además, por fecha en la carpeta Backup
  directorio2 = "W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\Archivos\\Backup\\"
  hoy = as.character(today())
  path_2 = paste(directorio2, hoy, "_", nombre_archivo, sep = "")
  
  drive_download(
    file = archivo,
    path = path_2,
    overwrite = TRUE)
  
  # Abro los archivos
  base[[k]] = read_xlsx(
    path_,
    sheet = "Base",
    col_names = TRUE,
    skip = 0,
    col_types = "text")
  
  base[[k]] = as.data.frame(base[[k]])
  
  colnames(base[[k]])[1] = "FECHA"
  colnames(base[[k]])[2] = "Nº ORDEN"
  
  base[[k]]$FECHA = as.Date(as.numeric(base[[k]]$FECHA), origin = "1899-12-30")
  base[[k]]$`Nº ORDEN` = as.numeric(base[[k]]$`Nº ORDEN`)
  base[[k]]$ALTURA = as.numeric(base[[k]]$ALTURA)
  #base[[k]] = base[[k]][, columnas]
  
  carga_datos[[k]] = read_xlsx(
    path_,
    sheet = "CargaDatos",
    col_names = TRUE,
    skip = 0,
    col_types = "text")
  
  
  carga_datos[[k]] = as.data.frame(carga_datos[[k]])
  
  colnames(carga_datos[[k]])[1] = "FECHA"
  colnames(carga_datos[[k]])[2] = "Nº ORDEN"
  
  carga_datos[[k]]$FECHA = as.Date(as.numeric(carga_datos[[k]]$FECHA), origin = "1899-12-30")
  carga_datos[[k]]$`Nº ORDEN` = as.numeric(carga_datos[[k]]$`Nº ORDEN`)
  carga_datos[[k]]$ALTURA = as.numeric(carga_datos[[k]]$ALTURA)
  #carga_datos[[k]] = carga_datos[[k]][, columnas]
  
  detalles[[k]] = read_xlsx(
    path_,
    sheet = "Detalles",
    col_names = FALSE,
    skip = 2,
    col_types = "text")
  
  colnames(detalles[[k]]) = c('Fecha',
                              'Coches remocionados con formulario - TM', 
                              'Coches remocionados con formulario - TT', 
                              'Formularios totales labrados - TM', 
                              'Formularios totales labrados - TT')
  detalles[[k]]$Fecha = as.Date(as.numeric(detalles[[k]]$Fecha), origin = "1899-12-30")
}

recorta_df = function(df){
  nombre = "Observaciones"
  df = df[, 1:(which(colnames(df) == nombre))]
}

dakota = base[[1]]
tacuari = base[[2]]
sarmiento = base[[3]]

dakota = dakota[which(!is.na(dakota$FECHA)),]
sarmiento = sarmiento[which(!is.na(sarmiento$FECHA)),]
tacuari = tacuari[which(!is.na(tacuari$FECHA)),]

dakota_cd = carga_datos[[1]]
tacuari_cd = carga_datos[[2]]
sarmiento_cd = carga_datos[[3]]

dakota = recorta_df(dakota)
dakota_cd = recorta_df(dakota_cd)
sarmiento = recorta_df(sarmiento)
sarmiento_cd = recorta_df(sarmiento_cd)
tacuari = recorta_df(tacuari)
tacuari_cd = recorta_df(tacuari_cd)

dakota_cd = dakota_cd[which(!is.na(dakota_cd$FECHA)),]
tacuari_cd = tacuari_cd[which(!is.na(tacuari_cd$FECHA)),]
sarmiento_cd = sarmiento_cd[which(!is.na(sarmiento_cd$FECHA)),]

colnames(dakota_cd) = colnames(dakota)
colnames(sarmiento_cd) = colnames(sarmiento)
colnames(tacuari) = colnames(tacuari_cd)

# Estos son los df que hay que subir en la carpeta de Formularios Nuevos (mensuales)
# -----------------------------------------------------------------------------------------------
dakota_up = rbind(dakota, dakota_cd)
tacuari_up = rbind(tacuari, tacuari_cd)
sarmiento_up = rbind(sarmiento, sarmiento_cd)

dakota_up$`HORA EGRESO` = as.numeric(dakota_up$`HORA EGRESO`)
dakota_up$`HORA INGRESO` = as.numeric(dakota_up$`HORA INGRESO`)

sarmiento_up$`HORA EGRESO` = as.numeric(sarmiento_up$`HORA EGRESO`)
sarmiento_up$`HORA INGRESO` = as.numeric(sarmiento_up$`HORA INGRESO`)

tacuari_up$`HORA EGRESO` = as.numeric(tacuari_up$`HORA EGRESO`)
tacuari_up$`HORA INGRESO` = as.numeric(tacuari_up$`HORA INGRESO`)

anio = as.character(year(today()))
mes = as.character(month(today()))
mes = str_pad(mes, 2, pad = "0")
periodo = paste("_",anio, mes, ".xlsx", sep = "")

directorio_mensuales = "W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\Archivos\\Mensuales\\"
id_carpeta_drive = "1wFBU9QKqmEAASBUO_RoIxJUk0TvXKl5x"

file_out = paste("Dakota",periodo, sep = "")
directorio_out = directorio_mensuales
salida = paste(directorio_out, file_out, sep = "")

write_xlsx(
  dakota_up,
  path = salida,
  col_names = TRUE,
  format_headers = TRUE,
  use_zip64 = FALSE
)

drive_upload(
  salida,
  path = as_id(id_carpeta_drive),
  overwrite = TRUE
)

file_out = paste("BRD Sarmiento",periodo, sep = "")
directorio_out = directorio_mensuales
salida = paste(directorio_out, file_out, sep = "")

write_xlsx(
  sarmiento_up,
  path = salida,
  col_names = TRUE,
  format_headers = TRUE,
  use_zip64 = FALSE
)

drive_upload(
  salida,
  path = as_id(id_carpeta_drive),
  overwrite = TRUE
)

file_out = paste("BRD Tacuari",periodo, sep = "")
directorio_out = directorio_mensuales
salida = paste(directorio_out, file_out, sep = "")

write_xlsx(
  tacuari_up,
  path = salida,
  col_names = TRUE,
  format_headers = TRUE,
  use_zip64 = FALSE
)

drive_upload(
  salida,
  path = as_id(id_carpeta_drive),
  overwrite = TRUE
)

# -----------------------------------------------------------------------------------------------

directorio = "W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\Archivos\\Crudos\\"
nombres_archivos = c("Dakota.txt", "Tacuari.txt", "Sarmiento.txt")

path_[1] = paste(directorio, nombres_archivos[1], sep = "")
path_[2] = paste(directorio, nombres_archivos[2], sep = "")
path_[3] = paste(directorio, nombres_archivos[3], sep = "")

historico_dakota = fread(path_[1], 
                         header = TRUE, 
                         sep = "\t",
                         encoding = 'UTF-8',
                         fill = TRUE,
                         na.strings = c("", NA),
                         colClasses = 'character')

historico_tacuari = fread(path_[2], 
                          header = TRUE, 
                          sep = "\t",
                          encoding = 'UTF-8',
                          fill = TRUE,
                          na.strings = c("", NA),
                          colClasses = 'character')
  
historico_sarmiento = fread(path_[3], 
                            header = TRUE, 
                            sep = "\t",
                            encoding = 'UTF-8',
                            fill = TRUE,
                            na.strings = c("", NA),
                            colClasses = 'character')

historico_dakota$FECHA = as.Date(historico_dakota$FECHA)
historico_tacuari$FECHA = as.Date(historico_tacuari$FECHA)
historico_sarmiento$FECHA = as.Date(historico_sarmiento$FECHA)

historico_dakota$`Nº ORDEN` = as.numeric(historico_dakota$`Nº ORDEN`)
historico_tacuari$`Nº ORDEN` = as.numeric(historico_tacuari$`Nº ORDEN`)
historico_sarmiento$`Nº ORDEN` = as.numeric(historico_sarmiento$`Nº ORDEN`)

historico_dakota$ALTURA = as.numeric(historico_dakota$ALTURA)
historico_tacuari$ALTURA = as.numeric(historico_tacuari$ALTURA)
historico_sarmiento$ALTURA = as.numeric(historico_sarmiento$ALTURA)

dakota = dplyr::bind_rows(historico_dakota, dakota)
tacuari = dplyr::bind_rows(historico_tacuari, tacuari)
sarmiento = dplyr::bind_rows(historico_sarmiento, sarmiento)

dakota = dakota %>% distinct(FECHA, `Nº ORDEN`, AGENTE, .keep_all = TRUE)
tacuari = tacuari %>% distinct(FECHA, `Nº ORDEN`, AGENTE, .keep_all = TRUE)
sarmiento = sarmiento %>% distinct(FECHA, `Nº ORDEN`, AGENTE, .keep_all = TRUE)

sarmiento$MAIL = substr(sarmiento$MAIL, 1, 50)
dakota$MAIL = substr(dakota$MAIL, 1, 50)
tacuari$MAIL = substr(tacuari$MAIL, 1, 50)

sarmiento$Observaciones = substr(sarmiento$Observaciones, 1, 50)
dakota$Observaciones = substr(dakota$Observaciones, 1, 50)
tacuari$Observaciones = substr(tacuari$Observaciones, 1, 50)

tacuari$DOMICILIO = substr(tacuari$DOMICILIO, 1, 100)
tacuari$`CALLE ACARREO` = substr(tacuari$`CALLE ACARREO`, 1, 100)

tacuari$`NOMBRE y APELLIDO` = substr(tacuari$`NOMBRE y APELLIDO`, 1, 100)
sarmiento$`NOMBRE y APELLIDO` = substr(sarmiento$`NOMBRE y APELLIDO`, 1, 100)
dakota$`NOMBRE y APELLIDO` = substr(dakota$`NOMBRE y APELLIDO`, 1, 100)

# Guardo los archivos crudos
directorio_out = "W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\Archivos\\Crudos\\"

file_out = paste("Dakota.txt", sep="")
salida = paste(directorio_out, file_out, sep = "")

fwrite(dakota, 
       salida, 
       append = FALSE, 
       row.names = FALSE, 
       col.names = TRUE,
       sep = "\t")

file_out = paste("Tacuari.txt", sep="")
salida = paste(directorio_out, file_out, sep = "")

fwrite(tacuari, 
       salida, 
       append = FALSE, 
       row.names = FALSE, 
       col.names = TRUE,
       sep = "\t")

file_out = paste("Sarmiento.txt", sep="")
salida = paste(directorio_out, file_out, sep = "")

fwrite(sarmiento, 
       salida, 
       append = FALSE, 
       row.names = FALSE, 
       col.names = TRUE,
       sep = "\t")

# Ahora guardemos un procesado unificando todas las bases

dakota$Base = "Dakota"
tacuari$Base = "Tacuari"
sarmiento$Base = "Sarmiento"

dakota_ = subset(dakota, select = columnas)
tacuari_ = subset(tacuari, select = columnas)
sarmiento_ = subset(sarmiento, select = columnas)

base = dplyr::bind_rows(dakota_, tacuari_, sarmiento_)

rownames(base) = NULL

# Leo el archivo de Nombres y CUITS
ref_agentes = fread("W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\Archivos\\Ref_Agentes_Sheets.txt")

base = merge(base, ref_agentes, by.x = "AGENTE", by.y = "Apellido_Nombre", all.x = TRUE)

# Hay agentes sin CUIT?
sin_cuit = base[which(is.na(base$CUIT)),]
sin_cuit = as.data.frame(unique(sin_cuit$AGENTE))
colnames(sin_cuit) = c("Agente_sin_cuit")

file_out = paste("Agentes_sin_cuit.txt", sep="")
directorio_out = "W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\Archivos\\"
salida = paste(directorio_out, file_out, sep = "")

fwrite(sin_cuit, 
       salida, 
       append = FALSE, 
       row.names = FALSE, 
       col.names = TRUE,
       sep = "\t")


# Guardo el consolidado

file_out = paste("Base_Consolidada.txt", sep="")
salida = paste(directorio_out, file_out, sep = "")

fwrite(base, 
       salida, 
       append = FALSE, 
       row.names = FALSE, 
       col.names = TRUE,
       sep = "\t")


# Trabajo las hojas de Detalles y guardo en el historico
archivos = c("Detalles_Dakota.xlsx", "Detalles_Tacuari.xlsx", "Detalles_Sarmiento.xlsx")
nombres = c("Dakota", "Tacuari", "Sarmiento")

df_detalles = NULL
for (base in (1:length(archivos))){
  base_detalles = detalles[[base]]
  base_detalles = base_detalles[which(base_detalles$Fecha <= today()), ]
  
  path_dt = paste("W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\Archivos\\Detalles\\", archivos[base], sep = "")
  base_detalles_hist = read_xlsx(
    path_dt,
    col_names = TRUE,
    skip = 0,
    col_types = "text")
  
  base_detalles_hist$Fecha = as.Date(as.numeric(base_detalles_hist$Fecha), origin = "1899-12-30")
  base_detalles_hist = rbind(base_detalles, base_detalles_hist)
  base_detalles_hist = base_detalles_hist %>% distinct(Fecha, .keep_all = TRUE)
  # Como pongo primero los datos nuevos (los que lee del sheets), para las fechas duplicadas se quedará siempre con la última info
  # al eliminar duplicados
  
  write_xlsx(
    base_detalles_hist,
    path = path_dt,
    col_names = TRUE,
    format_headers = TRUE,
    use_zip64 = FALSE
  )
  
  base_detalles_hist$Base = nombres[base]
  df_detalles = rbind(df_detalles, base_detalles_hist)
}

file_out = "Detalles_Consolidada.txt"
salida = paste(directorio_out, file_out, sep = "")

fwrite(df_detalles, 
       salida, 
       append = FALSE, 
       row.names = FALSE, 
       col.names = TRUE,
       sep = "\t")

# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Backup_Diario_GSheets"

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


