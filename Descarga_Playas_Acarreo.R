
# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Instalación condicional de paquetes

# Package names
packages <- c("dplyr", "lubridate", "data.table", "googlesheets4", "googledrive", "readxl", "stringi")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

char_fecha = function(caracter) {
  out = tryCatch(
    {
      caracter = as.IDate(caracter) 
    },
    error=function(cond) {
      return(caracter)
    }
  )    
  return(out)
}

char_fecha2 = function(caracter) {
  out = tryCatch(
    {
      caracter = parse_date_time(caracter,c('dmY','dmy'))
    },
    error=function(cond) {
      return(caracter)
    }
  )    
  return(out)
}

num_hora = function(hora){
  out = tryCatch(
    {
      if (class(hora)[1] == "character"){
        hora =  as.numeric(hora)
      }else{
        hora = hour(hora)/24 + minute(hora)/(24*60)
      } 
    },
    error=function(cond) {
      return(hora)
    }
  )    
  return(out)  
}


# Playas Operativas
 
# Playa Quinquela
# https://docs.google.com/spreadsheets/d/1mP5QOJ6fVQN3DDMlZjY_Hj_w069OLmzOg3TxSda2kQA/edit?usp=drive_web&ouid=118154184616569852830
 
# Playa Libres
# https://docs.google.com/spreadsheets/d/1GAChlNGX1m2pBC5lMQpFQo13ueWUoAGVEmpYrhq21zY/edit?usp=drive_web&ouid=118154184616569852830
 
# Playa Rio IV
# https://docs.google.com/spreadsheets/d/1o1EvkQHcYP-GQ8zX5nNpOKxy1xgOOKN0oH-Fw7fuZlM/edit
 
# Playa Sarmiento
# https://docs.google.com/spreadsheets/d/1F9f8zoGiICnBUz6L9HqrDLaX0FDb4gLx6nnUvu2oLi0/edit?usp=drive_web&ouid=118154184616569852830

drive_auth(email = "datos.ssgm@gmail.com")
id_quinquela = "1mP5QOJ6fVQN3DDMlZjY_Hj_w069OLmzOg3TxSda2kQA"
id_libres = "1GAChlNGX1m2pBC5lMQpFQo13ueWUoAGVEmpYrhq21zY"
id_rio4 = "1o1EvkQHcYP-GQ8zX5nNpOKxy1xgOOKN0oH-Fw7fuZlM"
id_sarmiento = "1F9f8zoGiICnBUz6L9HqrDLaX0FDb4gLx6nnUvu2oLi0" 

id = c(id_quinquela, id_libres, id_rio4, id_sarmiento)
nombres = c("PlayasOperativa_GoogleSheets_Quinquela.xlsx",
            "PlayasOperativa_GoogleSheets_Libres.xlsx",
            "PlayasOperativa_GoogleSheets_RioCuarto.xlsx",
            "PlayasOperativa_GoogleSheets_Sarmiento.xlsx")

nombres_out = c("Playas_Operativo_Quinquela.txt",
                "Playas_Operativo_Libres.txt",
                "Playas_Operativo_Rio4.txt",
                "Playas_Operativo_Sarmiento.txt")

nombres_data = c("data_quinquela",
                 "data_libres",
                 "data_rio4",
                 "data_sarmiento")

directorio = "W:\\Agentes Viales\\Playas_Acarreo\\"

nombres_columnas = c('Playa', 'Registro', 'Tipo_Vehiculo', 'Dominio', 'Placa', 'Nro_BUI', 'Playa_Privada', 
                     'En_Playa', 'Dias_Playa', 'Ped_Sec', 'Prohicion_Circular', 'Ingreso_Fecha', 'Ingreso_Hora', 
                     'Ingreso_Motivo', 'Cod_Infraccion', 'Ingreso_Inventario', 'Ingreso_Grua_Chofer', 'Ingreso_Daños', 
                     'Ingreso_Agente_Recibe', 'Vehiculo_Marca', 'Vehiculo_Modelo', 'Vehiculo_Posee_Placa', 
                     'Vehiculo_Chasis', 'Vehiculo_Motor', 'Vehiculo_Color', 'Infractor_Nombre', 
                     'Infraccion_Acta_Contravencional', 'Infraccion_Acta_Comprobacion', 'Infraccion_Boleta_Citacion', 
                     'Infraccion_Lugar_Remision', 'Infraccion_Agente_Labrante', 'Egreso_Agente_Entrega', 
                     'Egreso_Fecha', 'Egreso_Hora', 'Egreso_Retiro', 'Egreso_Nombre', 'Egreso_DNI', 
                     'Egreso_Conductor_Autorizado', 'Egreso_DNI_Autorizado', 'Egreso_Genero', 'Egreso_Forma_Retiro', 
                     'Egreso_Radicacion', 'Egreso_PROCOM', 'Egreso_Descargo', 'Interesado_Fecha', 'Interesado_Nombre', 
                     'Interesado_Telefono', 'Observaciones', 'Playa_Aux', 'Registro_Unificado')

# Descargo los archivos, los proceso y armo los df

data_consolidado = NULL

for (j in (1:length(id))){
  archivo = as_id(id[j])
  nombre_archivo = nombres[j]
  nombre_df = nombres_data[j]
  path_ = paste(directorio, nombre_archivo, sep = "")
  
  drive_download(
    file = archivo,
    path = path_,
    overwrite = TRUE)
  
  setwd(directorio)
  data = read_xlsx(
    nombre_archivo,
    sheet = "Datos",
    col_names = TRUE,
    skip = 2,
    .name_repair = "unique")
  
  # Las columnas que interesan y sin filas en blanco...
  data = data[,1:50]
  data = data[which(stri_length(data$DOMINIO) > 1),]
  
  # Lleno los vacios con NA
  data = data %>% mutate_if(is.character, list(~na_if(.,"")))
  
  # Nombres de columnas
  colnames(data) = nombres_columnas

  # Corrijo todos los campos de fecha y hora
  data$Egreso_Fecha = char_fecha(data$Egreso_Fecha)
  data$Ingreso_Fecha = char_fecha(data$Ingreso_Fecha)
  data$Interesado_Fecha = char_fecha(data$Interesado_Fecha)
  data$Egreso_Hora = num_hora(data$Egreso_Hora)
  data$Ingreso_Hora = num_hora(data$Ingreso_Hora)
  
  # Guardo los archivos de salida individuales
  
  file_out = nombres_out[j]
  directorio_out = directorio
  setwd(directorio_out)
  
  salida = paste(directorio_out, file_out, sep = "")
  
  fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  
  # Armo los dataframes de cada base
  assign(nombre_df, data)
  
  data_consolidado = rbind(data_consolidado, data)
}

# Guardo el archivo global
data_consolidado = unique(data_consolidado)

file_out = "Playas_Operativo.txt"
directorio_out = directorio
setwd(directorio_out)

salida = paste(directorio_out, file_out, sep = "")

fwrite(data_consolidado, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

#test = data_consolidado %>% group_by(En_Playa, Tipo_Vehiculo, Playa_Aux) %>% dplyr::summarise(cantidad = n())

# Abro el histórico
nombre_archivo = "Playas_Operativo_Historico.txt"
directorio_out = directorio
setwd(directorio_out)

archivo = paste(directorio_out, nombre_archivo, sep = "")

data_historico = fread(archivo,
                       col.names = nombres_columnas,
                       encoding = 'UTF-8',
                       sep = "\t")

# Lleno los vacios con NA
data_historico = data_historico %>% mutate_if(is.character, list(~na_if(.,"")))
data_historico$Observaciones = gsub('""', '"', data_historico$Observaciones)

data_consolidado_ = data_consolidado
data_historico_ = data_historico
data_consolidado_$id = seq.int(nrow(data_consolidado_))
data_historico_$id = seq.int(nrow(data_historico_))

data_historico = rbind(data_consolidado, data_historico)
data_historico_ = rbind(data_historico_, data_consolidado_)

data_historico$Egreso_Hora = round(data_historico$Egreso_Hora, 6)
data_historico$Ingreso_Hora = round(data_historico$Ingreso_Hora, 6)

data_historico_$Egreso_Hora = round(data_historico_$Egreso_Hora, 6)
data_historico_$Ingreso_Hora = round(data_historico_$Ingreso_Hora, 6)

#data_historico = distinct_all(data_historico)
#data_historico = unique(data_historico)
data_historico = data_historico %>% distinct(Registro_Unificado, .keep_all = TRUE)
data_historico_ = distinct_all(data_historico_)
ids = data_historico_ %>%
  group_by(id) %>%
  dplyr::summarise(cantidad=n())

data_historico_ = merge(data_historico_, ids, by.x = "id", by.y = "id", all.x = TRUE)
check = data_historico_[which(data_historico_$cantidad > 1),]

# Guardo el historico
nombre_archivo = "Playas_Operativo_Historico.txt"
directorio_out = directorio
setwd(directorio_out)

archivo = paste(directorio_out, nombre_archivo, sep = "")

fwrite(data_historico, archivo, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")

nombre_archivo = "Check_Duplicados.txt"
directorio_out = directorio
setwd(directorio_out)

archivo = paste(directorio_out, nombre_archivo, sep = "")

fwrite(check, archivo, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")


# --------------------------------------------------------------------------------------------
# Ahora el archivo de Gestión

# QUEDE ACÁ NLS 27/08/2021 -----------------------------------------------------------------------

drive_auth(email = "datos.ssgm@gmail.com")
id = '1ZRpLuW60VL2wNg6ml2CjZjsdOyZptPHmMiQrlyoQbFw'
archivo = as_id(id)
nombre_archivo = "PlayasGestion_GoogleSheets.xlsx"

# Descargo el archivo
directorio = "W:\\Agentes Viales\\Playas_Acarreo\\"
path_ = paste(directorio, nombre_archivo, sep = "")

# googledrive::drive_download(
#   file = archivo,
#   path = path_,
#   overwrite = TRUE)
# 
# # Leo el archivo y guardo la pestaña de "Datos"
# setwd(directorio)
# data = read_xlsx(
#   nombre_archivo,
#   sheet = "Datos",
#   col_names = TRUE,
#   skip = 2,
#   .name_repair = "unique")
# 
# 
# # Las columnas que interesan y sin filas en blanco...
# data = data[,1:44]

# 20210909 dejé de descargarlo porque tiraba error. Ahora simplemente lo leo online y voy directamente a esas 44 columnas iniciales

gs4_auth(email = "datos.ssgm@gmail.com")
data = googlesheets4::read_sheet(
  ss = id,
  sheet = "Datos",
  range = "Datos!A3:AR15000",
  col_names = TRUE,
  col_types = "c",
  .name_repair = "unique"
)

data = data[which(stri_length(data$DOMINIO) > 1),]

# Hoja Accesorios

ref_estados = googlesheets4::read_sheet(
  ss = id,
  sheet = "Accesorios",
  range = "Accesorios!C3:L15000",
  col_names = TRUE,
  col_types = "c",
  .name_repair = "unique"
)

# ref_estados = read_xlsx(
#   nombre_archivo,
#   sheet = "Accesorios",
#   col_names = TRUE,
#   skip = 2,
#   .name_repair = "unique")
# 
# ref_estados = ref_estados[,3:12]

ref_estados = ref_estados[which(stri_length(ref_estados$ESTADO) > 1 & stri_length(ref_estados$`N° y Estado`) > 1),]

# Abro el histórico
nombre_archivo = "Consolidado_Playas_Homogeneizado.xlsx"

data_historico = read_xlsx(
  nombre_archivo,
  sheet = "Historico",
  col_names = TRUE,
  skip = 2,
  .name_repair = "unique")

colnames(data_historico) = colnames(data)

nombres_columnas = c("Nro_Folio", "Playa", "Estado", "Ingreso_Fecha", "Dominio", "Ped_Sec", "P_Circ", "Doble_Titular", 
                     "Vencimiento_01_Plazo_Inicial", "Vencimiento_02_Respuesta_Correo", "Vencimiento_05_Notificacion_CD", "Vencimiento_03_Edicto", 
                     "Motivo", 
                     "Vehiculo_Tipo", "Vehiculo_Marca", "Vehiculo_Modelo", "Vehiculo_Placa", "Vehiculo_NroChasis", 
                     "Titular_Nombre", "Titular_DNI", "Titular_Calle", "Titular_Altura", "Titular_Localidad", "Titular_Provincia", "Titular_CodPostal", 
                     "SegundoTitular_Nombre", "SegundoTitular_DNI", "SegundoTitular_Direccion", 
                     "CD_DI", "CD_Notificacion", "CD_FechaEnvio", "CD_FechaNotificacion", 
                     "Edicto_NroEdicto", "Edicto_FechaEdicto", 
                     "Compactacion_Nro", "Compactacion_Nota", "Compactacion_Fecha", 
                     "ComentariosAndrea_Fecha", "ComentariosAndrea_Descripcion", 
                     "Cierre_Acta", "Cierre_FechaEgreso", "Cierre_FechaCompactacion", "Cierre_FechaUltimoContacto", "Cierre_Observaciones")



colnames(data) = nombres_columnas
colnames(data_historico) = nombres_columnas

## Corrijo todos los campos de fecha y hora

data$Nro_Folio = as.character(data$Nro_Folio)
data$CD_FechaEnvio = char_fecha2(data$CD_FechaEnvio)
data$CD_FechaNotificacion = char_fecha2(data$CD_FechaNotificacion)
data$Cierre_FechaCompactacion = char_fecha2(data$Cierre_FechaCompactacion)
data$Cierre_FechaEgreso = char_fecha2(data$Cierre_FechaEgreso)
data$Cierre_FechaUltimoContacto = char_fecha2(data$Cierre_FechaUltimoContacto)
data$Edicto_FechaEdicto = char_fecha2(data$Edicto_FechaEdicto)
data$Ingreso_Fecha = char_fecha2(data$Ingreso_Fecha)
data$Compactacion_Fecha = char_fecha2(data$Compactacion_Fecha)
data$ComentariosAndrea_Fecha = char_fecha2(data$ComentariosAndrea_Fecha)
data$Vencimiento_01_Plazo_Inicial = char_fecha2(data$Vencimiento_01_Plazo_Inicial)
data$Vencimiento_02_Respuesta_Correo = char_fecha2(data$Vencimiento_02_Respuesta_Correo)
data$Vencimiento_03_Edicto = char_fecha2(data$Vencimiento_03_Edicto)
data$Vencimiento_05_Notificacion_CD = char_fecha2(data$Vencimiento_05_Notificacion_CD)
data$Vehiculo_Modelo = as.character(data$Vehiculo_Modelo)

data_historico$CD_FechaEnvio = char_fecha(data_historico$CD_FechaEnvio)
data_historico$CD_FechaNotificacion = char_fecha(data_historico$CD_FechaNotificacion)
data_historico$Cierre_FechaCompactacion = char_fecha(data_historico$Cierre_FechaCompactacion)
data_historico$Cierre_FechaEgreso = char_fecha(data_historico$Cierre_FechaEgreso)
data_historico$Cierre_FechaUltimoContacto = char_fecha(data_historico$Cierre_FechaUltimoContacto)
data_historico$Edicto_FechaEdicto = char_fecha(data_historico$Edicto_FechaEdicto)
data_historico$Ingreso_Fecha = char_fecha(data_historico$Ingreso_Fecha)
data_historico$Compactacion_Fecha = char_fecha(data_historico$Compactacion_Fecha)
data_historico$ComentariosAndrea_Fecha = char_fecha(data_historico$ComentariosAndrea_Fecha)
data_historico$Ped_Sec = as.character(data_historico$Ped_Sec)
data_historico$P_Circ = as.character(data_historico$P_Circ)
data_historico$Doble_Titular = as.character(data_historico$Doble_Titular)
data_historico$Vencimiento_01_Plazo_Inicial = char_fecha(data_historico$Vencimiento_01_Plazo_Inicial)
data_historico$Titular_CodPostal = as.numeric(data_historico$Titular_CodPostal)
data_historico$SegundoTitular_Nombre = as.character(data_historico$SegundoTitular_Nombre)
data_historico$SegundoTitular_DNI = as.character(data_historico$SegundoTitular_DNI)
data_historico$SegundoTitular_Direccion = as.character(data_historico$SegundoTitular_Direccion)
data_historico$CD_FechaNotificacion = as.character(data_historico$CD_FechaNotificacion)
data_historico$Cierre_Acta = as.character(data_historico$Cierre_Acta)
data_historico$Cierre_FechaUltimoContacto = as.character(data_historico$Cierre_FechaUltimoContacto)
data_historico$Vencimiento_01_Plazo_Inicial = as.IDate(as.numeric(data_historico$Vencimiento_01_Plazo_Inicial), origin = "1899-12-30")
data_historico$Vencimiento_02_Respuesta_Correo = as.IDate(as.numeric(data_historico$Vencimiento_02_Respuesta_Correo), origin = "1899-12-30")
data_historico$Vencimiento_03_Edicto = as.IDate(as.numeric(data_historico$Vencimiento_03_Edicto), origin = "1899-12-30")
data_historico$Vencimiento_05_Notificacion_CD = as.IDate(as.numeric(data_historico$Vencimiento_05_Notificacion_CD), origin = "1899-12-30")

# Descarto los historicos que no tengan fecha de egreso pq supuestamente, lo que está en playa está en la nueva info
data_historico = data_historico[which(!is.na(data_historico$Cierre_FechaEgreso)),]

# Uno ambas bases
# sapply(data_historico, class)

# data_bck = data
#data_ = data
data = rbind(data, data_historico)

# Elimino duplicados por concat(nro_folio, dominio... pq no sabemos si en el histórico se duplicaron casos)
data$Concat_Folio_Dominio = paste(data$Nro_Folio, "_", data$Dominio, sep = "")
data = data %>% distinct(Concat_Folio_Dominio, .keep_all = TRUE)

# Quiero armar info duplicada para separar estado IN u OUT de vehículo
# Es decir, un mismo auto tendrá un registro para el ingreso y otro para el egreso

data_egresos = data[which(!is.na(data$Cierre_FechaEgreso)),]
data_ingresos = data[which(!is.na(data$Ingreso_Fecha)),]

data_ingresos$Accion = "IN"
data_egresos$Accion = "OUT"
data_ingresos$Fecha = data_ingresos$Ingreso_Fecha
data_egresos$Fecha = data_egresos$Cierre_FechaEgreso

data_consolidado = rbind(data_ingresos, data_egresos)

# Archivo de bases

nombre_archivo = "W:\\Agentes Viales\\Ref\\Ref_Bases.xlsx"
ref_bases = read_xlsx(
  nombre_archivo,
  col_names = TRUE,
  .name_repair = "unique")

data_consolidado$Playa = toupper(data_consolidado$Playa)
ref_bases$Bases = toupper(ref_bases$Bases)

data_consolidado = merge(data_consolidado, ref_bases, by.x = "Playa", by.y = "Bases", all.x = TRUE)
data_consolidado$Playa = data_consolidado$`Base Corta`
data_consolidado$`Base Corta` = NULL
data_consolidado$`Tipo de Base` = NULL
data_consolidado$Lat = NULL
data_consolidado$Lon = NULL
data_consolidado$Dirección = NULL
data_consolidado$Capacidad_Autos = NULL
data_consolidado$Capacidad_Motos = NULL

# Guardo los archivos de salida


file_out = "Playas_Gestion.txt"
directorio_out = directorio
setwd(directorio_out)

salida = paste(directorio_out, file_out, sep = "")

fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = ",")

file_out = "Playas_Tablero.txt"
directorio_out = directorio
setwd(directorio_out)

salida = paste(directorio_out, file_out, sep = "")

fwrite(data_consolidado, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = ",")

file_out = "ref_estados.txt"
directorio_out = "W:\\Agentes Viales\\Playas_Acarreo\\Ref\\"
setwd(directorio_out)

salida = paste(directorio_out, file_out, sep = "")

fwrite(ref_estados, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = ",")

# Vamos a guardar algunas de las listas que hay que homogeneizar luego

localidades = as.data.frame(unique(data_consolidado$Titular_Localidad))
marca_vehiculo = as.data.frame(unique(data_consolidado$Vehiculo_Marca))
motivos = as.data.frame(unique(data_consolidado$Motivo))

file_localidades = "W:\\Agentes Viales\\Playas_Acarreo\\Ref\\ref_localidades.txt"
file_marcas = "W:\\Agentes Viales\\Playas_Acarreo\\Ref\\ref_marcas.txt"
file_motivos = "W:\\Agentes Viales\\Playas_Acarreo\\Ref\\ref_motivos.txt"

fwrite(localidades, file_localidades, append = FALSE, row.names = FALSE, col.names = TRUE, sep = ",")
fwrite(marca_vehiculo, file_marcas, append = FALSE, row.names = FALSE, col.names = TRUE, sep = ",")
fwrite(motivos, file_motivos, append = FALSE, row.names = FALSE, col.names = TRUE, sep = ",")


# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Descarga_Playas_Acarreo"

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



# Escribo el historico en la hoja de gestión (únicamente casos cerrados)
# --------------------------------------------------------------------------------
data_cerrados = data[which(!is.na(data$Cierre_FechaEgreso)),]

gs4_auth(email = "datos.ssgm@gmail.com")
id_gestion = id
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

tryCatch({
  celda = paste("B1")
  
  range_write(
    ss = id_gestion,
    data = df_hora_guardado,
    sheet = 'Cerrados',
    range = celda,
    col_names = FALSE,
    reformat = FALSE
  )
  
  celda = paste("A3")
  
  range_write(
    ss = id_gestion,
    data = data_cerrados,
    sheet = 'Cerrados',
    range = celda,
    col_names = TRUE,
    reformat = FALSE
  )
}, error=function(e){})
