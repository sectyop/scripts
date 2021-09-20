# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(googledrive)
library(readxl)
library(data.table)
library(googlesheets4)
library(lubridate)

# Busco archivos con el nombre de Nomina 1.2.xlsx
#drive_get("Nomina 1.2.xlsx")

drive_auth(email = "datos.ssgm@gmail.com")

# Obtengo el ID del que quiero utilizar
id_nomina = "1Ei1gdQaQg6pA-3IYKMkz9jtEifEv0qgo"
archivo = as_id(id_nomina)
nombre_archivo = "Nomina 1.2.xlsx"

# Descargo el archivo
directorio = "W:\\Agentes Viales\\Nomina\\"
path_ = paste(directorio, nombre_archivo, sep = "")

drive_download(
  file = archivo,
  path = path_,
  overwrite = TRUE)

# Leo el archivo y lo guardo
setwd(directorio)
data_nomina = read_xlsx(
                  nombre_archivo,
                  sheet = "Datos CAT",
                  col_names = TRUE,
                  .name_repair = "unique")

data_nomina = data_nomina[,2:37]
nombres_columnas = c('Apellido_', 'Nombre', 'CUIT', 'Email', 'Email gobierno', 'Sexo', 'Telefono particular', 
                     'Telefono casa', 'Telefono ht', 'Estado Civil', 'Base', 'Area', 'Cargo', 'Turno', 
                     'Gerencia', 'Funcion', 'Funcion especifica', 'Fecha de ingreso modalidad actual', 
                     'Fecha de ingreso al GCBA', 'ID Sial', 'Ficha', 'Tipo de contrato', 'Tipo de inscripcion', 
                     'Monto factura', 'Estado', 'Fecha baja', 'Comentario baja', 'Estudios', 'Observacion', 'Profesion', 
                     'Hora entrada', 'Hora salida', 'Rotativo', 'Eximido', 'Domicilio constituido', 'Domicilio real')

colnames(data_nomina) = nombres_columnas

# 2021/01/13 - Agrego fecha de ingreso al GCBA a aquellos que no tenían
file_ref_fechas = "W:\\Agentes Viales\\Nomina\\Agentes_Sin_Fecha_Ingreso.txt"
ref_fechas = fread(file_ref_fechas)
ref_fechas$CUIT = as.character(ref_fechas$CUIT)

data_nomina = merge(data_nomina, ref_fechas, by.x = "CUIT", by.y = "CUIT", all.x = TRUE)
data_nomina$`Fecha de ingreso al GCBA` = ifelse(!is.na(data_nomina$Fecha_Ingreso), 
                                                data_nomina$Fecha_Ingreso, 
                                                data_nomina$`Fecha de ingreso al GCBA`)
data_nomina$Fecha_Ingreso = NULL

# Guardo el archivo
file_out = "Nomina_CAT.txt"

directorio_out = directorio
setwd(directorio_out)

salida = paste(directorio_out, file_out, sep = "")

fwrite(data_nomina, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# Vamos por las tablas de referencia
directorio_out = "W:\\Agentes Viales\\Ref\\"
setwd(directorio)

# 01 - Bases
ref_bases = read_xlsx(
  nombre_archivo,
  sheet = "Tipo",
  col_names = TRUE,
  range = "A3:D100",
  .name_repair = "unique")

ref_bases = ref_bases[!is.na(ref_bases$Bases),]

file_out = "Ref_Bases.txt"
salida = paste(directorio_out, file_out, sep = "")

fwrite(ref_bases, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# 02 - Areas
ref_areas = read_xlsx(
  nombre_archivo,
  sheet = "Tipo",
  col_names = TRUE,
  range = "G3:I100",
  .name_repair = "unique")

ref_areas = ref_areas[!is.na(ref_areas$Area),]

file_out = "Ref_Areas.txt"
salida = paste(directorio_out, file_out, sep = "")

fwrite(ref_areas, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# 03 - Contratos
ref_contratos = read_xlsx(
  nombre_archivo,
  sheet = "Tipo",
  col_names = TRUE,
  range = "k3:M100",
  .name_repair = "unique")

ref_contratos = ref_contratos[!is.na(ref_contratos$Contrato),]
ref_contratos$Orden = ifelse(ref_contratos$`Tipo de contrato 2` == "LOYS", 3,
                             ifelse(ref_contratos$`Tipo de contrato 2` == "PT", 2, 1))

file_out = "Ref_Contratos.txt"
salida = paste(directorio_out, file_out, sep = "")

fwrite(ref_contratos, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# 04 - Estudios
ref_estudios = read_xlsx(
  nombre_archivo,
  sheet = "Tipo",
  col_names = TRUE,
  range = "p3:r1000",
  .name_repair = "unique")

ref_estudios = ref_estudios[!is.na(ref_estudios$Titulo),]

file_out = "Ref_Estudios.txt"
salida = paste(directorio_out, file_out, sep = "")

fwrite(ref_estudios, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# 05 - Otros Estados
ref_otros_estados = read_xlsx(
  nombre_archivo,
  sheet = "Otros Estados",
  col_names = TRUE,
  range = "b3:j1000",
  .name_repair = "unique")

ref_otros_estados = ref_otros_estados[!is.na(ref_otros_estados$Apellido),]

file_out = "Ref_Otros_Estados.txt"
salida = paste(directorio_out, file_out, sep = "")

fwrite(ref_otros_estados, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

# 06 - Licencias

# ---- Busco archivos con el nombre de PLANILLAS LICENCIAS CAT
#drive_get("Planilla Licencias CAT")

# ---- Obtengo el ID del que quiero utilizar
id_licencias = "1884JrHXmRz098bbHB6Ci5ol1wd5efcENiHcQXZ-s0G0"
archivo = as_id(id_licencias)
nombre_archivo = "Planilla Licencias CAT.xlsx"

# ---- Descargo el archivo
directorio = "W:\\Agentes Viales\\Nomina\\"
path_ = paste(directorio, nombre_archivo, sep = "")

drive_download(
  file = archivo,
  path = path_,
  overwrite = TRUE)

# ---- Leo el archivo y lo guardo
setwd(directorio)
licencias = read_xlsx(
  nombre_archivo,
  sheet = "Datos",
  col_names = TRUE,
  .name_repair = "unique")

licencias =licencias[!is.na(licencias$...1),]

nombre_columnas = as.list(licencias[1,])
colnames(licencias) = nombre_columnas
filas = nrow(licencias)
licencias = licencias[2:filas,]

licencias =licencias[!(licencias$CUIT == "Na"),]
licencias$CUIT = as.numeric(licencias$CUIT)

licencias$`Fecha Inicio Lic` = as.numeric(licencias$`Fecha Inicio Lic`)
licencias$`Fecha Inicio Lic` = as.Date(licencias$`Fecha Inicio Lic`, origin = "1899-12-30")

licencias$`Fecha Fin Lic` = as.numeric(licencias$`Fecha Fin Lic`)
licencias$`Fecha Fin Lic` = as.Date(licencias$`Fecha Fin Lic`, origin = "1899-12-30")

licencias$`Tareas Livianas Desde` = as.numeric(licencias$`Tareas Livianas Desde`)
licencias$`Tareas Livianas Desde` = as.Date(licencias$`Tareas Livianas Desde`, origin = "1899-12-30")

licencias$`Tareas Livianas Hasta` = as.numeric(licencias$`Tareas Livianas Hasta`)
licencias$`Tareas Livianas Hasta` = as.Date(licencias$`Tareas Livianas Hasta`, origin = "1899-12-30")

file_out = "Ref_Licencias.txt"
salida = paste(directorio_out, file_out, sep = "")

fwrite(licencias, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


# 07 - Codigos Presentismo

# ---- Busco archivos con el nombre de Códigos presentismo

# ---- Obtengo el ID del que quiero utilizar
id_codigos = "1LoVOSwBoNVAQWQJEUoEyqD2Gvysg7Ma6yAE5nmxJsJg"
archivo = as_id(id_codigos)
nombre_archivo = "Ref_Codigos.xlsx"

# ---- Descargo el archivo
directorio = "W:\\Agentes Viales\\Ref\\"
path_ = paste(directorio, nombre_archivo, sep = "")

drive_download(
  file = archivo,
  path = path_,
  overwrite = TRUE)

nuevos_codigos = fread("W:\\Agentes Viales\\Ref\\nuevas_codigos.txt",
                       header = FALSE)

if(nrow(nuevos_codigos) == 0){
  nuevos_codigos = data.frame(matrix(data = NA, nrow = 0, ncol = 1))
}

colnames(nuevos_codigos) = c("Codigos Nuevos")

# Lo escribo en la hoja Nuevos_Codigos
gs4_auth(email = "datos.ssgm@gmail.com")
celda = "A1:A500"

tryCatch({
  range_write(
    ss = id_codigos,
    data = nuevos_codigos,
    sheet = 'Nuevos_Codigos',
    range = celda,
    col_names = TRUE,
    reformat = FALSE
  )
}, error=function(e){})

# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Descarga_Nomina_y_referencias"

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
