# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librer�as
library(lubridate)
library(data.table)
library(dplyr)
library(gmailr)
library(glue)
library(googlesheets4)

gm_auth_configure(
  path = "W:\\Colectivos\\Larga_Distancia\\credenciales.json"
)

# Abro el archivo de solicitudes
directorio = "W:\\Colectivos\\Larga_Distancia\\"
setwd(directorio)
file = "Solicitudes_Procesado.txt"
file_ = paste(directorio, file, sep = "")

solicitudes = fread(file_,
                    encoding = 'UTF-8',
                    sep = "\t")

# Abro el archivo de mails enviados
file = "mails_enviados.txt"
file_ = paste(directorio, file, sep = "")

mails_enviados = fread(file_,
                      encoding = 'UTF-8',
                      sep = "\t")

# Abro el archivo de servicios aprobados
file = "servicios_aprobados.txt"
file_ = paste(directorio, file, sep = "")

servicios_aprobados = fread(file_,
                       encoding = 'UTF-8',
                       sep = "\t")

# Empiezo a armar el df para tener la info de los mails a enviar
mails_enviar = solicitudes

# 1) Veo cu�les fueron aprobados
mails_enviar = merge(mails_enviar, servicios_aprobados, 
                     by.x = "ID", by.y = "ID")

# 2) Veo cu�les no fueron enviados
mails_enviados$enviado = rep(1, nrow(mails_enviados))

mails_enviar = merge(mails_enviar, mails_enviados,
                     by.x = "ID", by.y = "ID",
                     all.x = TRUE)

mails_enviar = mails_enviar[which(is.na(mails_enviar$enviado)),]
#mails_enviar = mails_enviar[which(mails_enviar$email == "jlopezsaez@buenosaires.gob.ar"),]

# Guardo el archivo con los que quedan por enviar
df_mail_enviar = as.data.frame(mails_enviar$ID)
colnames(df_mail_enviar) = c("ID")

# Guardo el archivo de salida
nombre_salida = "mails_restan_enviar.txt"
directorio_out = directorio
salida = paste(directorio_out, nombre_salida, sep = "")
fwrite(df_mail_enviar, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")



# 3) Me quedo �nicamente con los campos que interesan
mails_enviar = mails_enviar %>% select(ID,
                                       email,
                                       fecha_egreso_caba,
                                       fecha_ingreso_caba,
                                       provincia_origen,
                                       provincia_destino,
                                       nombre_empresa,
                                       dominio)

# 4) Defino el tipo de servicio
mails_enviar$tipo_servicio = ifelse(is.na(mails_enviar$fecha_ingreso_caba),
                                    "Origen_CABA",
                                    "Destino_CABA")

# 5) Elimino aquellos registros que no van (no tienen mail)
mails_enviar = mails_enviar[which(!is.na(mails_enviar$email)),]

# Contenido que sacamos del mail

# Resoluci�n RESOL-2020-570-GCABA-SECTOP por medio de la cual se autorizan los arribos y partidas de servicios de transporte:
#   https://drive.google.com/file/d/105wCFuW_cOONkWm9KPmZVggInyeIIua4/view?usp=sharing

# 6) Defino la funci�n de env�o de mails
prepara_mail = function(fecha, empresa, dominio, id, tipo_servicio, email){
  if(tipo_servicio == "Origen_CABA"){
    contenido = glue(
                "Estimado:
                
                Se informa que su tr�mite para egresar de la Ciudad Aut�noma de Buenos Aires ha sido AUTORIZADO. A continuaci�n se confirman los datos del servicio solicitado.
                
                Recuerde que saliendo de la terminal se pedir� el n�mero de ID que se manda en este mail.
                
                Fecha y horario de partida: {fecha}
                Dominio: {dominio}
                Raz�n social: {empresa}
                
                ID: {id}
                
                Saludos.
                
                Video con el protocolo para el ingreso a la Ciudad Aut�noma de Buenos Aires: https://drive.google.com/file/d/1EjUoEJy2wjqQDNwyzVuQ-xgE70t1SVEf/view
                

                Este es un mail generado autom�ticamente, rogamos por favor no contestar este mail, si tiene alguna consulta enviar un mail aparte a esta direcci�n: infolargadistancia@buenosaires.gob.ar"
                    )
  }else{
    contenido = glue(
                "Estimado:
                
                Se informa que su tr�mite para ingresar a la Ciudad Aut�noma de Buenos Aires ha sido AUTORIZADO. A continuaci�n se confirman los datos del servicio solicitado.
                
                Recuerde que llegado a la terminal se pedir� el n�mero de ID que se manda en este mail.
                
                Fecha y horario de arribo: {fecha}
                Dominio: {dominio}
                Raz�n social: {empresa}
                
                ID: {id}
                
                Saludos.
                
                Video con el protocolo para el ingreso a la Ciudad Aut�noma de Buenos Aires: https://drive.google.com/file/d/1EjUoEJy2wjqQDNwyzVuQ-xgE70t1SVEf/view
                
                
                Este es un mail generado autom�ticamente, rogamos por favor no contestar este mail, si tiene alguna consulta enviar un mail aparte a esta direcci�n: infolargadistancia@buenosaires.gob.ar"
                    )
  }
  
  #asunto = paste("Autorizaci�n uso terminal Dellepiane - ID: ", id, sep ="")
  
  mensaje = gm_mime() %>%
    gm_to(email) %>%
    gm_from("largadistancia.gcba@gmail.com") %>%
    gm_subject("Autorizaci�n Servicio Larga Distancia") %>%
    gm_text_body(contenido)


  return (mensaje)
}

# 6) Env�o los mails

j = 1
#i=10
filas = nrow(mails_enviar)
id_anexar_mails_enviados = data.frame(matrix(ncol = 1, nrow = filas))

if(filas>0){
  for (i in 1:filas){
    tipo_servicio_ = mails_enviar$tipo_servicio[i]
    id_ = mails_enviar$ID[i]
    empresa_ = mails_enviar$nombre_empresa[i]
    dominio_ = mails_enviar$dominio[i]
    
    if(nchar(dominio_) < 4){
      dominio_ = "S/D"
    }
    
    email_ = mails_enviar$email[i]
    
    if (tipo_servicio_ == "Origen_CABA"){
      fecha_ = mails_enviar$fecha_egreso_caba[i]
    }else{
      fecha_ = mails_enviar$fecha_ingreso_caba[i]
    }
    
    email = prepara_mail(fecha = fecha_,
                         empresa = empresa_,
                         dominio = dominio_,
                         id = id_,
                         tipo_servicio = tipo_servicio_,
                         email = email_)
    
    tryCatch({
      gm_send_message(email)
      colnames(id_anexar_mails_enviados) = c("ID")
      id_anexar_mails_enviados$ID[j] = id_
      j = j + 1
    }, error=function(e){})
  }
}


# Actualizo archivo de mails enviados
file = "mails_enviados.txt"
file_ = paste(directorio, file, sep = "")

id_anexar_mails_enviados = id_anexar_mails_enviados[which(!is.na(id_anexar_mails_enviados$ID)),]
id_anexar_mails_enviados = as.data.frame(id_anexar_mails_enviados)

mails_enviados$enviado = NULL
colnames(id_anexar_mails_enviados) = colnames(mails_enviados)

mails_enviados = rbind(mails_enviados, id_anexar_mails_enviados)
mails_enviados = unique(mails_enviados)

fwrite(mails_enviados, file_, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")



# Escribo status ejecuci�n en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Solicitudes_TransporteLD_ENVIO_MAILS"

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





