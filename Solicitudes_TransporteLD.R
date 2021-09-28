# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)
library(stringr)
library(googledrive)
library(readxl)
library(stringi)
library(openssl)
library(googlesheets4)


drive_auth(email = "nacho.ls@gmail.com")

id_nomina = drive_get("Solicitud de permisos larga distancia")$id  
nombre_archivo = "Solicitud_de_permisos_larga_distancia.xlsx"

id_nomina_nuevo = "1HGajvXxi2eleUP-ZKT-OnGM8F7sfmbHz95vvQKzb0cE"
nombre_archivo_nuevo = "Solicitud_de_permisos_larga_distancia_conRetiro.xlsx"


archivo = as_id(id_nomina)
archivo_nuevo = as_id(id_nomina_nuevo)

# Descargo el archivo
directorio = "W:\\Colectivos\\Larga_Distancia\\"
path_ = paste(directorio, nombre_archivo, sep = "")
path_nuevo = paste(directorio, nombre_archivo_nuevo, sep = "")

# Si es post 1/7/2021, no lo bajo más... me quedo con lo que está
fecha_corte = as.Date("2021-07-01")
fecha = today()

if (fecha < fecha_corte + 2){
  drive_download(
    file = archivo,
    path = path_,
    overwrite = TRUE)
}

drive_download(
  file = archivo_nuevo,
  path = path_nuevo,
  overwrite = TRUE)


nombres_columnas = c('fechahora_respuesta', 'tipo_servicio', 'password', 'email', 'cod_cnrt_turismo', 'nombre_empresa_turismo', 
                     'dominio_turismo', 'provincia_origen_turismo', 'ciudad_origen_turismo', 'fecha_ingreso_caba_turismo', 
                     'fecha_egreso_caba_turismo', 'cod_cnrt_sp', 'nombre_empresa_sp', 'dominio_sp', 'provincia_origen_sp', 
                     'ciudad_origen_sp', 'fecha_ingreso_caba_sp', 'provincia_destino_turismo', 'ciudad_destino_turismo', 
                     'eliminar_08', 'eliminar_09', 'eliminar_10', 'origen_destino_caba_turismo', 'para_liniers_turismo_oc', 
                     'para_liniers_turismo_dc', 'origen_destino_caba_sp', 'eliminar_01', 'para_liniers_sp_dc', 'eliminar_02', 
                     'eliminar_03', 'eliminar_04', 'eliminar_05', 'para_liniers_sp_oc', 'provincia_destino_sp', 'ciudad_destino_sp', 
                     'fecha_egreso_caba_sp', 'eliminar_06', 'eliminar_07', 'ID', 'permanencia_caba')

nombres_columnas_nuevo = c('fechahora_respuesta', 'email', 'password', 'tipo_servicio', 'cod_cnrt_turismo', 'nombre_empresa_turismo', 
                           'dominio_turismo', 'origen_destino_caba_turismo', 'para_liniers_turismo_oc', 'provincia_destino_turismo', 
                           'ciudad_destino_turismo', 'fecha_egreso_caba_turismo', 'para_liniers_turismo_dc', 'provincia_origen_turismo', 
                           'ciudad_origen_turismo', 'fecha_ingreso_caba_turismo', 'cod_cnrt_sp', 'nombre_empresa_sp', 'dominio_sp', 
                           'origen_destino_caba_sp', 'para_liniers_sp_dc', 'provincia_origen_sp', 'ciudad_origen_sp', 'fecha_ingreso_caba_sp', 
                           'para_liniers_sp_oc', 'provincia_destino_sp', 'ciudad_destino_sp', 'fecha_egreso_caba_sp')
 
# # Leo el archivo y lo guardo
setwd(directorio)
base = read_xlsx(
  nombre_archivo,
  sheet = "Respuestas salida Form",
  col_names = nombres_columnas,
  col_types = "text",
  skip = 0)


base_nueva = read_xlsx(
  nombre_archivo_nuevo,
  sheet = "Respuestas de formulario 1",
  col_names = nombres_columnas_nuevo,
  col_types = "text",
  skip = 0)

# Elimino columnas innecesarias del archivo viejo
columnas_eliminar = c('eliminar_01', 'eliminar_02', 'eliminar_03', 'eliminar_04', 'eliminar_05', 'eliminar_06', 'eliminar_07', 'eliminar_08', 
                      'eliminar_09', 'eliminar_10', 'ID', 'permanencia_caba')

base[ , columnas_eliminar] = list(NULL)

# Uno ambos datasets

# 2021/07/02 por ahora lo saco porque hay gente que sigue cargando en el viejo...
#fecha_corte = ymd_hms("2021-07-01 00:00:00") + hours(3)
#base = base[which(base$fechahora_respuesta < fecha_corte),]

base = rbind(base, base_nueva)

base$fechahora_respuesta = as.numeric(base$fechahora_respuesta)
base$fechahora_respuesta = as.Date(base$fechahora_respuesta, origin = "1899-12-30")
base$fechahora_respuesta = as.POSIXct(base$fechahora_respuesta) + hours(3)

base$fecha_egreso_caba_sp = as.numeric(base$fecha_egreso_caba_sp)
base$fecha_egreso_caba_sp = as.Date(base$fecha_egreso_caba_sp, origin = "1899-12-30")
base$fecha_egreso_caba_sp = as.POSIXct(base$fecha_egreso_caba_sp) + hours(3) + seconds(1)

base$fecha_ingreso_caba_sp = as.numeric(base$fecha_ingreso_caba_sp)
base$fecha_ingreso_caba_sp = as.Date(base$fecha_ingreso_caba_sp, origin = "1899-12-30")
base$fecha_ingreso_caba_sp = as.POSIXct(base$fecha_ingreso_caba_sp) + hours(3) + seconds(1)

base$fecha_egreso_caba_turismo = as.numeric(base$fecha_egreso_caba_turismo)
base$fecha_egreso_caba_turismo = as.Date(base$fecha_egreso_caba_turismo, origin = "1899-12-30")
base$fecha_egreso_caba_turismo = as.POSIXct(base$fecha_egreso_caba_turismo) + hours(3) + seconds(1)

base$fecha_ingreso_caba_turismo = as.numeric(base$fecha_ingreso_caba_turismo)
base$fecha_ingreso_caba_turismo = as.Date(base$fecha_ingreso_caba_turismo, origin = "1899-12-30")
base$fecha_ingreso_caba_turismo = as.POSIXct(base$fecha_ingreso_caba_turismo) + hours(3) + seconds(1)

fecha_corte = as.Date("2021-05-12")
base = base[which(base$fechahora_respuesta > fecha_corte),]

# set.seed(1)
# base$ID = stri_rand_strings(nrow(base), 10)

base$texto = paste(base$fechahora_respuesta, base$email, sep = "")
base$ID = md5(base$texto)
base$texto = NULL


base$fecha_egreso_caba = str_c(str_replace_na(base$fecha_egreso_caba_sp, replacement = ""), 
                               str_replace_na(base$fecha_egreso_caba_turismo, replacement = ""), 
                               sep = "")

base$fecha_ingreso_caba = str_c(str_replace_na(base$fecha_ingreso_caba_sp, replacement = ""), 
                                str_replace_na(base$fecha_ingreso_caba_turismo, replacement = ""), 
                                sep = "")

base$provincia_origen = str_c(str_replace_na(base$provincia_origen_sp, replacement = ""), 
                              str_replace_na(base$provincia_origen_turismo, replacement = ""), 
                              sep = "")

base$provincia_destino = str_c(str_replace_na(base$provincia_destino_sp, replacement = ""), 
                               str_replace_na(base$provincia_destino_turismo, replacement = ""), 
                               sep = "")

base$ciudad_origen = str_c(str_replace_na(base$ciudad_origen_sp, replacement = ""), 
                           str_replace_na(base$ciudad_origen_turismo, replacement = ""), 
                           sep = "")

base$ciudad_destino = str_c(str_replace_na(base$ciudad_destino_sp, replacement = ""), 
                            str_replace_na(base$ciudad_destino_turismo, replacement = ""), 
                            sep = "")

base$ciudad_origen = toupper(base$ciudad_origen)
base$ciudad_destino = toupper(base$ciudad_destino)

base$nombre_empresa = toupper(str_c(str_replace_na(base$nombre_empresa_turismo, replacement = ""), 
                                    str_replace_na(base$nombre_empresa_sp, replacement = ""), 
                                    sep = ""))

base$cod_cnrt = str_c(str_replace_na(base$cod_cnrt_sp, replacement = ""), 
                      str_replace_na(base$cod_cnrt_turismo, replacement = ""), 
                      sep = "")

base$dominio = toupper(str_c(str_replace_na(base$dominio_turismo, replacement = ""), 
                             str_replace_na(base$dominio_sp, replacement = ""), 
                             sep = ""))

base$para_liniers = toupper(str_c(str_replace_na(base$para_liniers_sp_dc, replacement = ""), 
                                  str_replace_na(base$para_liniers_sp_oc, replacement = ""),
                                  str_replace_na(base$para_liniers_turismo_oc, replacement = ""),
                                  str_replace_na(base$para_liniers_turismo_dc, replacement = ""),
                                  sep = ""))

columnas_eliminar = c('ciudad_destino_sp', 'ciudad_destino_turismo', 'ciudad_origen_sp', 
                      'ciudad_origen_turismo', 'cod_cnrt_sp', 'cod_cnrt_turismo', 'dominio_sp', 
                      'dominio_turismo', 'fecha_egreso_caba_sp', 'fecha_egreso_caba_turismo', 
                      'fecha_ingreso_caba_sp', 'fecha_ingreso_caba_turismo', 'nombre_empresa_sp', 
                      'nombre_empresa_turismo', 'origen_destino_caba_sp', 'origen_destino_caba_turismo', 
                      'para_liniers_sp_dc', 'para_liniers_sp_oc', 'para_liniers_turismo_dc', 
                      'para_liniers_turismo_oc', 'provincia_destino_sp', 'provincia_destino_turismo', 
                      'provincia_origen_sp', 'provincia_origen_turismo')

base[ , columnas_eliminar] = list(NULL)

# Redefino las categorias viejas de "Para en Liners"

base$para_liniers = ifelse(base$para_liniers == "SI",
                           "DELLEPIANE - CON PARADA EN LINIERS",
                          ifelse(base$para_liniers == "NO" | base$para_liniers == "",
                                  "DELLEPIANE",
                                  base$para_liniers))

#base_bck = base
#base = base_bck

base$fecha_ingreso_caba = ymd_hms(base$fecha_ingreso_caba)
base$fecha_egreso_caba = ymd_hms(base$fecha_egreso_caba)

base = base[which(base$email != "nacho.ls@gmail.com"),]

# Guardo el archivo de salida
nombre_salida = "Solicitudes_Procesado.txt"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(base, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "nacho.ls@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Solicitudes_TransporteLD"

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
