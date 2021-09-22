# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Instalación condicional de paquetes

# Package names
packages <- c("dplyr", "lubridate", "data.table", "googledrive", "readxl", "gtools", 
              "googlesheets4", "readr", "stringi")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

# Abro base consolidada
file = "Base_Consolidada.txt"
directorio = "W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\Archivos\\"
file = paste(directorio, file, sep = "")
base = fread(file, encoding = 'UTF-8')
base$`CALLE ACARREO` = toupper(base$`CALLE ACARREO`)
base$ubicacion = paste(base$`CALLE ACARREO`, base$ALTURA, sep = " ")
ubicaciones = base %>% select(`CALLE ACARREO`, ALTURA, ubicacion)

### Abro normalizador de calles
file = "Ref_CorrigeCalles.txt"
directorio = "W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\"
file = paste(directorio, file, sep = "")
ref_calles = fread(file, encoding = 'UTF-8')
ref_calles = unique(ref_calles)

ubicaciones = merge(ubicaciones, ref_calles, by.x = "CALLE ACARREO", by.y = "Calle", all.x = TRUE)

### Guardo nuevas calles?
nuevas_calles = as.data.frame(unique(ubicaciones[which(is.na(ubicaciones$Calle_Oficial)),1]))
file_out = "Calles_Nuevas.txt"
directorio_out = "W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\"
salida = paste(directorio_out, file_out, sep = "")
fwrite(nuevas_calles, salida, append = FALSE, row.names = FALSE, col.names = FALSE, sep = "\t")


ubicaciones$Nro_Redondeado100 = ifelse(ubicaciones$ALTURA > 0,
                                       round(ubicaciones$ALTURA / 100, 0)*100,
                                       ubicaciones$ALTURA)

ubicaciones$Direccion_Final = paste(ubicaciones$Calle_Oficial, ubicaciones$Nro_Redondeado100, sep = " ")

# Saco las que no tienen altura (o es 0) porque seguro no las encontrare
ubicaciones = ubicaciones[which(ubicaciones$Nro_Redondeado100 > 0),]

# También las que no tienen nombre de calle
ubicaciones$largo_calle = nchar(ubicaciones$`CALLE ACARREO`)
ubicaciones = ubicaciones[which(ubicaciones$largo_calle > 0),]
ubicaciones = ubicaciones[which(ubicaciones$`CALLE ACARREO` != 'NA'),]

ubicaciones = as.data.frame(cbind(ubicaciones$ubicacion, ubicaciones$Direccion_Final))
colnames(ubicaciones) = c("Direccion", "Direccion_Corregida")

direcciones = ubicaciones
ubicaciones = NULL

### Abro las ubicaciones ya geolocalizadas
file = "Ref_Ubicaciones.txt"
directorio = "W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\"
file = paste(directorio, file, sep = "")
data_ubicaciones = fread(file,
                         encoding = 'UTF-8')


direcciones = merge(direcciones, data_ubicaciones, by.x = "Direccion_Corregida", by.y = "Direccion", all.x = TRUE)

direcciones$lat = ifelse(direcciones$lat < 0,
                         direcciones$lat,
                         NA)

direcciones$lon = ifelse(direcciones$lon < 0,
                         direcciones$lon,
                         NA)

direcciones_buscar = direcciones[which(is.na(direcciones$lon)),]


ubicaciones = as.data.frame(unique(direcciones_buscar$Direccion_Corregida))
colnames(ubicaciones) = c("Direccion")

filas = nrow(ubicaciones)
ubicaciones$url = gsub(" ", "+",paste("https://www.google.com/maps/place/",
                                      ubicaciones$Direccion, 
                                      " buenos aires", sep =""))
numeros = rep(100,nrow(ubicaciones))
ubicaciones$lat = numeros
ubicaciones$lon = numeros

for (i in (1:filas)){
  vuelta = paste("Numero de fila: ", i, " // ", 100*round(i/filas,4), "% del total", sep = "")
  print(vuelta)
  
  url = as.character(ubicaciones[i,2])
  
  tryCatch({
    ad = read.csv(url,
                  header = FALSE)
    
    ad_ = ad[ad[,3] %like% "-34", 2:3]
    ad_ = ad_[1,]
    
    lat = as.numeric(substr(ad_$V3,1,12))
    lon = as.numeric(substr(ad_$V2,1,12))
    
  }, error=function(e){})
  
  if(is.na(lat) || length(lat) == 0 || round(lat,5) == -34.57427){
    lat = 100
  }
  
  
  if(is.na(lon) || length(lon) == 0 || round(lon,5) == -58.47443){
    lon = 100
  }
  
  ubicaciones[i,3] = lat
  ubicaciones[i,4] = lon
}

# Guardo el consolidado
ubicaciones = dplyr::bind_rows(data_ubicaciones, ubicaciones)
ubicaciones$Direccion = toupper(ubicaciones$Direccion)
ubicaciones = ubicaciones %>% distinct(Direccion, .keep_all = TRUE)

# Guardo el archivo
file_out = "Ref_Ubicaciones.txt"
directorio_out = "W:\\Agentes Viales\\Playas_Acarreo\\Playas_Privadas\\"
salida = paste(directorio_out, file_out, sep = "")

fwrite(ubicaciones, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")



# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "nacho.ls@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Backup_Diario_GSheets_ObtieneCoordenadas"

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

