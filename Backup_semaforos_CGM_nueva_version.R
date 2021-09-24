# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Cargo librerías
library(lubridate)
library(data.table)
library(readxl)
library(googledrive)
library(stringr)
library(dplyr)
library(googlesheets4)

fecha_cambio = as.IDate("2021-07-01")
hoy = today()

#if (hoy >= fecha_cambio){
  directorio = "W:\\CGM\\Backup_Semaforos\\"
  setwd(directorio)
  
  # Descargamos la base desde el Google Drive
  drive_auth(email = "nacho.ls@gmail.com")
  gs4_auth(email = "nacho.ls@gmail.com")
  
  #id_semaforos = drive_get("PLANILLA SEMÁFOROS CAT/CGM - PRUEBA 2")$id
  #id_semaforos = "19r3XwQvET1i4M2ZR_07MLn-uEosUb2bSYlDDxwK5s8c"
  #esta anterior es la planilla vieja que se deja de usar el 01/07/2021
  id_semaforos = "1iN0U4NIVAaoY8lun060dhKrAoTeu08cWF8PXLDWQNgI"
  archivo = as_id(id_semaforos)
  
  nombre_archivo = "Planilla_Semaforos.xlsx"
  #nombre_archivo = "Planilla_Semaforos_testNueva.xlsx"
  # Descargo el archivo
  path_ = paste(directorio, nombre_archivo, sep = "")
  
  drive_download(
    file = archivo,
    path = path_,
    overwrite = TRUE)
  
  # Leo el archivo
  data_temp = read_xlsx(path = path_,
                        .name_repair = "unique",
                        sheet = "MONITOREO Y GESTIÓN",
                        #col_types = clases,
                        skip = 9)
  
  # Selecciono el rango H-AM (columnas 8 a 39) y solo las filas no vacías
  data_temp = data_temp[which(!is.na(data_temp$`INICIO SUCESO`)), (8:39)]
  
  # Para el acumulado anexado, me quedo únicamente los cerrados
  data_temp_acum = data_temp[which(data_temp$`ESTADO SUCESO` == "Cerrado"),]
  
  # Calculo la hora y fecha y agrego la columna
  ahora = Sys.time()
  
  ahora_lista = data.frame(rep(ahora, nrow(data_temp)))
  colnames(ahora_lista) = c("FechaHora_Backup")
  
  base = cbind(data_temp, ahora_lista)
  
  
  # Guardo el archivo anexando la nueva data
  
  file_out = paste("Backup_Semaforos_CGM.txt", sep="")
  
  directorio_out = "W:\\CGM\\Backup_Semaforos\\"
  setwd(directorio_out)
  
  salida = paste(directorio_out, file_out, sep = "")
  
  # --- Abro el histórico
  
  historico_acum = fread(salida,
                         encoding = 'UTF-8',
                         #colClasses = clases,
                         sep = "\t")
  
  historico_acum$`HORA LLEGADA CAT` = as.character(historico_acum$`HORA LLEGADA CAT`)
  historico_acum$`HORA RETIRO CAT` = as.character(historico_acum$`HORA RETIRO CAT`)
  historico_acum$`FIN SUCESO` = as.character(historico_acum$`FIN SUCESO`)
  historico_acum$`HORA ORDEN CONF` = as.character(historico_acum$`HORA ORDEN CONF`)
  # historico_acum$`MEDIO COMUNICACIÓN` = as.character(historico_acum$`MEDIO COMUNICACIÓN`)
  # historico_acum$`BASE ASIGNADA`= as.character(historico_acum$`BASE ASIGNADA`)
  # historico_acum$`CÁMARAS LÍNEAS` = as.numeric(historico_acum$`CÁMARAS LÍNEAS`)

  data_temp_acum$`HORA LLEGADA CAT` = as.character(data_temp_acum$`HORA LLEGADA CAT`)
  data_temp_acum$`HORA RETIRO CAT` = as.character(data_temp_acum$`HORA RETIRO CAT`)
  data_temp_acum$`FIN SUCESO` = as.character(data_temp_acum$`FIN SUCESO`)
  data_temp_acum$`HORA ORDEN CONF` = as.character(data_temp_acum$`HORA ORDEN CONF`)
  
  # historico_acum$FechaHora_Backup = NULL
  # historico_acum = historico_acum[which(historico_acum$ESTADO == "Cerrado"), ]
  
  names(data_temp_acum)[names(data_temp_acum) == "CRITICIDAD...22"] = "CRITICIDAD...20"
  names(data_temp_acum)[names(data_temp_acum) == "CRITICIDAD...23"] = "CRITICIDAD...20"
  names(data_temp_acum)[names(data_temp_acum) == "CRITICIDAD...24"] = "CRITICIDAD...20"
  
  # Ordeno las columnas de la nueva bajada de acuerdo al historico
  col_order = colnames(historico_acum)
  data_temp_acum = data_temp_acum[,col_order]
  
  colnames(data_temp_acum) == colnames(historico_acum)
  
  # fill in non-overlapping columns with NAs
  #bck1 = data_temp_acum
  #bck2 = historico_acum
  
  # df1 = data_temp_acum
  # df2 = historico_acum
  # 
  # historico_final = rbind(
  #   data.frame(c(df1, sapply(setdiff(names(df2), names(df1)), function(x) NA))),
  #   data.frame(c(df2, sapply(setdiff(names(df1), names(df2)), function(x) NA)))
  # )
  #historico_acum = rbind(historico_acum, data_temp_acum)
  
  data_temp_acum$`DESCRIPCIÓN DE SUCESO` = substr(data_temp_acum$`DESCRIPCIÓN DE SUCESO`, start = 1, stop = 50)
  historico_acum$`DESCRIPCIÓN DE SUCESO` = substr(historico_acum$`DESCRIPCIÓN DE SUCESO`, start = 1, stop = 50)
  historico_final = rbind(historico_acum, data_temp_acum)
  
  # agregar = c("ESTADO AGENTES", "ESTADO SUCESO")
  # nombres = colnames(historico_acum)
  # nombres = c(nombres, agregar)
  # 
  # colnames(historico_final) = nombres

  historico_final = historico_final %>% distinct(`CALLE 1`,
                                                 `CALLE 2`,
                                                 `TIPO DE FALLA`,
                                                 INPUT,
                                                 ESTADO,
                                                 `COBERTURA AGENTES`,
                                                 `INICIO SUCESO`, .keep_all = TRUE)
  
  #historico_acum = unique(historico_acum)
  
  fwrite(historico_final, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  #fwrite(data_temp_acum, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  
  # Guardo el archivo solo
  
  anio = as.character(year(ahora))
  mes = as.character(str_pad(month(ahora), 2, pad = "0"))
  dia = as.character(str_pad(day(ahora), 2, pad = "0"))
  hora = as.character(str_pad(hour(ahora), 2, pad = "0"))
  minuto = as.character(str_pad(minute(ahora), 2, pad = "0"))
  
  fecha = paste(anio, mes, dia, "_", hora, minuto,sep = "")
  
  file_out2 = paste(fecha, "_Backup_Semaforos_CGM.txt", sep="")
  
  directorio_out2 = "W:\\CGM\\Backup_Semaforos\\Archivos\\"
  setwd(directorio_out2)
  
  salida2 = paste(directorio_out2, file_out2, sep = "")
  
  fwrite(base, salida2, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  
  # Escribo en el sheets el momento del guardado
  df_hora_guardado = as.data.frame(Sys.time() - hours(3))
  colnames(df_hora_guardado) = "Ultimo Backup"
  
  tryCatch({
    range_write(
      ss = id_semaforos,
      data = df_hora_guardado,
      sheet = 'MONITOREO Y GESTIÓN',
      range = 'R1:R2',
      col_names = TRUE,
      reformat = FALSE
    )
  }, error=function(e){})
  
  # Vamos a procesar la hoja "Base de datos" donde están geolocalizados los cruces semafóricos
  
  # Leo el archivo
  
  data_geo = read_xlsx(path = path_,
                       .name_repair = "unique",
                       sheet = "BASE DE DATOS",
                       #col_types = clases,
                       skip = 0)
  
  data_geo$interseccion = ifelse(data_geo$`CALLE 1` < data_geo$`CALLE 2`,
                                 paste(data_geo$`CALLE 1`, "y", data_geo$`CALLE 2`),
                                 paste(data_geo$`CALLE 2`, "y", data_geo$`CALLE 1`))
  
  data_geo = data_geo %>% select(interseccion, LATITUD, LONGITUD, CRITICIDAD)
  
  data_geo = unique(data_geo)
  
  file_out3 = "Ref_Geoloc_Semaforos.txt"
  directorio_out3 = "W:\\CGM\\Backup_Semaforos\\"
  setwd(directorio_out3)
  
  salida3 = paste(directorio_out3, file_out3, sep = "")
  
  fwrite(data_geo, salida3, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")
  
  
  # Escribo status ejecución en hoja "Status Datos SSGM"
  gs4_auth(email = "nacho.ls@gmail.com")
  id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
  df_hora_guardado = as.data.frame(Sys.time() - hours(3))
  colnames(df_hora_guardado) = "FechaHoraActual"
  
  codigo_r = "Backup_semaforos_CGM"
  
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
#}
