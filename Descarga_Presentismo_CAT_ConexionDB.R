#------------------------------------------------------------------------------------------
# DB Nomina CAT
# 12/02/2021
# Acceso a la base de datos de la nómina CAT
# Armo un df con la información de presentismo
# Nómina seguiré tomando del archivo que trabajar en el CAT (lo podría tomar de acá...)
# porque entiendo que hacen modificaciones sobre la data cruda que está aquí
#------------------------------------------------------------------------------------------

# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(dplyr)
library(RPostgreSQL)
library(DBI)
library(RPostgres)
library(stringr)
library(googlesheets4)
library(readxl)

db = 'presentismo'
host_db = '10.78.7.23'
db_port = '5432'
db_user = 'consulta'
db_password = 'consulta1202'

con = dbConnect(RPostgres::Postgres(), 
                dbname = db, 
                host = host_db, 
                port = db_port, 
                user = db_user, 
                password = db_password)

lista_tablas = 0
lista_tablas = dbListTables(con)
#lista_tablas
#length(lista_tablas)

# meto todo dentro de un if viendo si hay elementos, para que el script no corra si la conexión no fue exitosa

if (length(lista_tablas) > 1){
  
  tipos_presentismos = dbReadTable(con, "tipos_presentismos")
  presentismos = dbReadTable(con, "presentismos")
  agentes = dbReadTable(con, "agentes")
  
  # ------------------------------------------- check agentes con y sin la foto cargada...
  con_fotos = agentes[which(agentes$avatar != 'default.jpg'),]
  sin_fotos = agentes[which(agentes$avatar == 'default.jpg'),]
  fwrite(con_fotos, "W:\\Agentes Viales\\Nomina\\lista_agentes_con_fotos.txt", row.names = FALSE, col.names = TRUE, sep = "\t")
  fwrite(sin_fotos, "W:\\Agentes Viales\\Nomina\\lista_agentes_sin_fotos.txt", row.names = FALSE, col.names = TRUE, sep = "\t")
  # ------------------------------------------------------------
  lista_tablas
  numeros_cat = dbReadTable(con, "temp_numeroscat")
  
  data = merge(presentismos, tipos_presentismos, by.x = "id_tipo_presentismo", by.y = "id", all.x = TRUE)
  data = merge(data, agentes, by.x = "id_agente", by.y = "id", all.x = TRUE)
  
  # data = data %>% select(id_agente, nombre, apellido, dni, cuit,
  #                        id_tipo_presentismo, codigo, descripcion, injustificado.y,
  #                        fecha)
  
  data = data %>% select(id_agente, nombre, apellido, dni, cuit,
                         id_tipo_presentismo, codigo, descripcion, injustificado.x,
                         fecha)
  
  data = unique(data)
  
  fecha_corte = today()
  
  data = data[which(data$fecha < fecha_corte),]
  
  # 20210608: me fijo si aparecen nuevos codigos..................................................
  codigos = as.data.frame(unique(data$codigo))
  colnames(codigos) = c("Codigos")
  
  file_ref = "W:\\Agentes Viales\\Ref\\Ref_Codigos.xlsx"
  ref_codigos = read_excel(file_ref)
  
  codigos = merge(codigos, ref_codigos, by.x = "Codigos", by.y = "CODIGO", all.x = TRUE)
  codigos = codigos[is.na(codigos$Contrato),]
  codigos = as.data.frame(codigos$Codigos)
  
  nuevos_codigos = "W:\\Agentes Viales\\Ref\\nuevos_codigos.txt"
  fwrite(codigos, nuevos_codigos, append = FALSE, row.names = FALSE, col.names = FALSE)
  # Listo.........................................................................................

  file_out = "Data_Presentismo.txt"
  directorio_out = "W:\\Agentes Viales\\Nomina\\"
  salida = paste(directorio_out, file_out, sep = "")
  
  fwrite(data, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  
  # -----------------------------------------------------------------------------------------------------
  # Preparo el archivo de nónima tal como está el Nomina 1.2.xlsx
  
  estudios = dbReadTable(con, "estudios")
  estudios = estudios[which(is.na(estudios$deleted_at)),]
  estudios = estudios %>% distinct(id_agente, .keep_all = TRUE)
  
  contratos = dbReadTable(con, "contratos")
  contratos = contratos[which(is.na(contratos$deleted_at)),]
  contratos = contratos %>% distinct(id_agente, .keep_all = TRUE)
  
  operativos = dbReadTable(con, "operativos")
  operativos = operativos[which(is.na(operativos$deleted_at)),]
  operativos = operativos %>% distinct(id_agente, .keep_all = TRUE)
  
  horarios = dbReadTable(con, "horarios")
  horarios = horarios %>% distinct(id, .keep_all = TRUE)
  
  turnos = dbReadTable(con, "turnos")
  turnos = turnos %>% distinct(id, .keep_all = TRUE)
  
  estado_contratos =  dbReadTable(con, "estado_contratos")
  estado_contratos = estado_contratos %>% distinct(id, .keep_all = TRUE)
  
  domicilios = dbReadTable(con, "domicilios")
  domicilios = domicilios[which(is.na(domicilios$deleted_at)),]
  domicilios = domicilios %>% distinct(id, .keep_all = TRUE)
  domicilios$domicilio = paste(domicilios$calle, " ",
                               domicilios$numero, " ",
                               domicilios$departamento, " ", 
                               domicilios$piso,
                               " - ",
                               domicilios$barrio,
                               " - ",
                               "CP ", domicilios$codigo_postal,
                               " - ",
                               domicilios$provincia,
                               sep = "")
  domicilios$domicilio = toupper(domicilios$domicilio)
  
  domicilios_constituidos = domicilios[which(domicilios$constituido == TRUE),]
  domicilios_constituidos_bck = domicilios_constituidos
  domicilios_constituidos = domicilios_constituidos %>% select(id_agente, domicilio)
  colnames(domicilios_constituidos) = c("id_agente", "domicilio_constituido")
  domicilios_constituidos = domicilios_constituidos %>% distinct(id_agente, .keep_all = TRUE)
  
  domicilios_reales =  domicilios[which(domicilios$constituido != TRUE),]
  domicilios_reales_bck = domicilios_reales
  domicilios_reales = domicilios_reales %>% select(id_agente, domicilio)
  colnames(domicilios_reales) = c("id_agente", "domicilio_real")
  domicilios_reales = domicilios_reales %>% distinct(id_agente, .keep_all = TRUE)
  
  
  funciones =  dbReadTable(con, "funciones")
  funciones = funciones %>% distinct(id, .keep_all = TRUE)
  
  gerencias =  dbReadTable(con, "gerencias")
  gerencias = gerencias %>% distinct(id, .keep_all = TRUE)
  
  cargos =  dbReadTable(con, "cargos")
  cargos = cargos %>% distinct(id, .keep_all = TRUE)
  
  tipo_contratos =  dbReadTable(con, "tipo_contratos")
  tipo_contratos = tipo_contratos %>% distinct(id, .keep_all = TRUE)
  
  areas =  dbReadTable(con, "areas")
  areas = areas %>% distinct(id, .keep_all = TRUE)
  
  bases =  dbReadTable(con, "bases")
  bases = bases %>% distinct(id, .keep_all = TRUE)
  
  nomina = agentes
  
  nomina = merge(agentes, estudios, by.x = "id", by.y = "id_agente", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge(nomina, contratos, by.x = "id", by.y = "id_agente", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge (nomina, operativos, by.x = "id", by.y = "id_agente", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge(nomina, domicilios_constituidos, by.x = "id", "id_agente", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge(nomina, domicilios_reales, by.x = "id", "id_agente", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge(nomina, horarios, by.x = "id", by.y = "id", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge(nomina, turnos, by.x = "id_turno", "id", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge(nomina, estado_contratos, by.x = "id_estado_contrato", "id", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge(nomina, funciones, by.x = "id_funcion", "id", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge(nomina, gerencias, by.x = "id_gerencia", "id", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge(nomina, cargos, by.x = "id_cargo", "id", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge(nomina, tipo_contratos, by.x = "id_tipo_contrato", "id", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge(nomina, areas, by.x = "id_area", by.y = "id", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  nomina = merge(nomina, bases, by.x = "id_base", by.y = "id", all.x = TRUE)
  names(nomina) = make.unique(names(nomina), sep="_")
  
  # Elimino algunas columnas accesorias
  nomina = nomina %>% select(-contains("created"))
  nomina = nomina %>% select(-contains("updated"))
  nomina = nomina %>% select(-contains("deleted"))
  
  
  nomina$carrera_completo = paste("Carrera: ", nomina$carrera,
                                  " - Institucion: ", nomina$institucion,
                                  " - Estado: ", nomina$estado.x,
                                  " - Nivel: ", nomina$nivel,
                                  sep = "")
  
  nomina_bck = nomina
  
  seleccion_columnas_1 = c('cuit', 'apellido', 'nombre.x', 'email', 'email_gobierno', 'sexo', 
                           'telefono_particular', 'telefono_casa', 'telefono_ht', 
                           'estado_civil', 'nombre.y_2', 'nombre.x_2', 'nombre.y_1', 
                           'codigo.x', 'nombre.x_1', 'nombre.y', 'funcion_especifica', 
                           'fecha_ingreso', 'fecha_ingreso_gobierno', 'id_sial', 'ficha', 
                           'descripcion', 'tipo_inscripcion', 'monto', 'estado.y', 'fecha_fin', 
                           'comentario.y', 'carrera_completo', 'observacion', 'profesion', 
                           'hora_entrada', 'hora_salida', 'rotativo', 'eximido', 
                           'domicilio_constituido', 'domicilio_real', 'dni', 'fecha_nacimiento', 
                           'numero_cat')
  # nomina = nomina %>% select(cuit, apellido, nombre.x, email, email_gobierno, sexo, telefono_particular, 
  #                            telefono_casa, telefono_ht, estado_civil, nombre.y_2, nombre.x_2, nombre.y_1, 
  #                            codigo.x, nombre.x_1, nombre.y, funcion_especifica, fecha_ingreso, 
  #                            fecha_ingreso_gobierno, id_sial, ficha, descripcion, tipo_inscripcion, 
  #                            monto, estado.y, fecha_fin, comentario.y, carrera_completo, observacion, 
  #                            profesion, hora_entrada, hora_salida, rotativo, eximido, domicilio_constituido, 
  #                            domicilio_real, dni, fecha_nacimiento)
  nomina = nomina %>% select(all_of(seleccion_columnas_1))
  
  nombres_columnas = c("CUIT", "Apellido_", "Nombre", "Email", "Email gobierno", "Sexo", "Telefono particular", 
                       "Telefono casa", "Telefono ht", "Estado Civil", "Base", "Area", "Cargo", "Turno", 
                       "Gerencia", "Funcion", "Funcion especifica", "Fecha de ingreso modalidad actual", 
                       "Fecha de ingreso al GCBA", "ID Sial", "Ficha", "Tipo de contrato", "Tipo de inscripcion", 
                       "Monto factura", "Estado", "Fecha baja", "Comentario baja", "Estudios", "Observacion", 
                       "Profesion", "Hora entrada", "Hora salida", "Rotativo", "Eximido", 
                       "Domicilio constituido", "Domicilio real", "DNI", "Fecha de nacimiento",
                       "Numero_CAT")
  
  colnames(nomina) = nombres_columnas
  
  
  # Agrego la fecha de la bajada
  ahora = Sys.time()
  fecha_nomina = ymd_hms(ahora)
  nomina$Fecha_Nomina = fecha_nomina
  
  # Guardo el archivo
  ahora = fecha_nomina
  anio = as.character(year(ahora))
  mes = as.character(str_pad(month(ahora), 2, pad = "0"))
  dia = as.character(str_pad(day(ahora), 2, pad = "0"))
  
  fecha = paste(anio, mes, dia, "_", sep = "")
  file_out = paste(fecha, "Nomina_CAT.txt", sep="")
  file_out2 = "Nomina_CAT_Actual.txt"
  
  
  directorio_out = "W:\\Agentes Viales\\Nomina\\Bajadas_Nomina\\"
  directorio_out2 = "W:\\Agentes Viales\\Nomina\\"
  
  salida = paste(directorio_out, file_out, sep = "")
  salida2 = paste(directorio_out2, file_out2, sep = "")
  
  fwrite(nomina, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  fwrite(nomina, salida2, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  
  # 23/08/2021
  # JIR Pide una nueva bajada con nueva info, adicional a la que ya disponemos en la nomina...
  domicilios_constituidos_bck = domicilios_constituidos_bck %>% distinct(id_agente, .keep_all = TRUE)
  domicilios_reales_bck = domicilios_reales_bck %>% distinct(id_agente, .keep_all = TRUE)
  
  columnas_domicilios = c('id_agente', 'calle', 'numero', 'departamento', 'piso', 'barrio', 'provincia')
  
  domicilios_constituidos_bck = domicilios_constituidos_bck %>% select(all_of(columnas_domicilios))
  domicilios_reales_bck = domicilios_reales_bck %>% select(all_of(columnas_domicilios))
  
  nombres_constituidos = c('id_agente', 'Constituido_Calle', 'Constituido_Nro', 'Constituido_Depto',
                           'Constituido_Piso', 'Constituido_Barrio', 'Constituido_Provincia')
  nombres_reales = c('id_agente', 'Real_Calle', 'Real_Nro', 'Real_Depto',
                    'Real_Piso', 'Real_Barrio', 'Real_Provincia')
  colnames(domicilios_constituidos_bck) = nombres_constituidos
  colnames(domicilios_reales_bck) = nombres_reales
  
  nomina_bck = merge(nomina_bck, domicilios_constituidos_bck, by.x = "id", by.y = "id_agente", all.x = TRUE)
  nomina_bck = merge(nomina_bck, domicilios_reales_bck, by.x = "id", by.y = "id_agente", all.x = TRUE)
  
  seleccion_columnas_2 = c('cuit', 'apellido', 'nombre.x', 'email', 'email_gobierno', 'sexo', 
                           'telefono_particular', 'telefono_casa', 'telefono_ht', 
                           'estado_civil', 'nombre.y_2', 'nombre.x_2', 'nombre.y_1', 
                           'codigo.x', 'nombre.x_1', 'nombre.y', 'funcion_especifica', 
                           'fecha_ingreso', 'fecha_ingreso_gobierno', 'id_sial', 'ficha', 
                           'descripcion', 'tipo_inscripcion', 'monto', 'estado.y', 'fecha_fin', 
                           'comentario.y', 'carrera', 'institucion', 'estado.x', 'nivel', 'observacion', 'profesion', 
                           'hora_entrada', 'hora_salida', 'rotativo', 'eximido', 
                           'dni', 'fecha_nacimiento', 'numero_cat', 'Constituido_Calle', 'Constituido_Nro', 'Constituido_Depto',
                           'Constituido_Piso', 'Constituido_Barrio', 'Constituido_Provincia', 'Real_Calle', 'Real_Nro', 'Real_Depto',
                           'Real_Piso', 'Real_Barrio', 'Real_Provincia')
  
  nomina_bck = nomina_bck %>% select(all_of(seleccion_columnas_2))
  
  nombres_columnas = c("CUIT", "Apellido_", "Nombre", "Email", "Email gobierno", "Sexo", "Telefono particular", 
                       "Telefono casa", "Telefono ht", "Estado Civil", "Base", "Area", "Cargo", "Turno", 
                       "Gerencia", "Funcion", "Funcion especifica", "Fecha de ingreso modalidad actual", 
                       "Fecha de ingreso al GCBA", "ID Sial", "Ficha", "Tipo de contrato", "Tipo de inscripcion", 
                       "Monto factura", "Estado", "Fecha baja", "Comentario baja", "Estudios_Carrera",
                       "Estudios_Institucion", "Estudios_Estado", "Estudios_Nivel", "Observacion", 
                       "Profesion", "Hora entrada", "Hora salida", "Rotativo", "Eximido", 
                       "DNI", "Fecha de nacimiento", "NUmero_CAT", 'Constituido_Calle', 'Constituido_Nro', 'Constituido_Depto',
                       'Constituido_Piso', 'Constituido_Barrio', 'Constituido_Provincia', 'Real_Calle', 'Real_Nro', 'Real_Depto',
                       'Real_Piso', 'Real_Barrio', 'Real_Provincia')
  
  colnames(nomina_bck) = nombres_columnas
  
  file_out3 = "Nomina_CAT_Adicional.txt"
  directorio_out3 = "W:\\Agentes Viales\\Nomina\\Bajada_Nomina_Adicional\\"
  
  salida3 = paste(directorio_out3, file_out3, sep = "")
  
  fwrite(nomina_bck, salida3, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  
  # Guardo la fechahora de la ultima corrida exitosa
  data_fecha = as.data.frame(now() - hours(3))
  colnames(data_fecha) = c("Ult_Actualizacion_DB")
  file_out3 = "FechaUltimaActualizacionDB.txt"
  directorio_out3 = "W:\\Agentes Viales\\Nomina\\"
  salida3 = paste(directorio_out3, file_out3, sep = "")
  
  fwrite(data_fecha, salida3, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  
  # Lo guardo en un google sheets también
  gs4_auth(email = "datos.ssgm@gmail.com")
  df_hora_guardado = data_fecha
  
  id_sheets = "1rBKpw_G_D0F27sQAVEOi9U4AphFsEwwSPicRGPJB0Zk"
  celda = "B2"
  
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
  gs4_auth(email = "datos.ssgm@gmail.com")
  id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
  df_hora_guardado = as.data.frame(Sys.time() - hours(3))
  colnames(df_hora_guardado) = "FechaHoraActual"
  
  codigo_r = "Descarga_Presentismo_CAT_ConexionDB"
  
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




# Guardo el acumulado...

# file_out = "Nomina_CAT_Acumulado.txt"
# 
# directorio_out = "W:\\Agentes Viales\\Nomina\\Bajadas_Nomina\\"
# salida = paste(directorio_out, file_out, sep = "")
# 
# fwrite(nomina, salida, append = TRUE, row.names = FALSE, col.names = FALSE, sep = "\t")
# 
# # Cierro la conexión con la base de datos...




