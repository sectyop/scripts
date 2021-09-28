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
library(reshape)
library(httpuv)

googledrive::drive_auth(email = "datos.ssgm@gmail.com")

# Busco archivos con el nombre de Operacion Conteo Diario
#drive_get("OPERACION - Conteo diario")

# Busco archivos con el nombre de Operacion Conteo Diario
id_nomina = drive_get("Status-Conteo Ecobici")$id
archivo = as_id(id_nomina)
nombre_archivo = "Conteo_Diario_Ecobici_NuevoFormato.xlsx"

# Descargo el archivo
directorio = "W:\\Ecobici\\Operacion\\"
path_ = paste(directorio, nombre_archivo, sep = "")

drive_download(
  file = archivo,
  path = path_,
  overwrite = TRUE)

# Leo el archivo y lo guardo
setwd(directorio)
base = read_xlsx(
  nombre_archivo,
  sheet = "Diario",
  col_names = FALSE,
  skip = 1)

base = as.data.frame(base)

# Guardo la tabla de abonos
abonos = read_xlsx(
  nombre_archivo,
  sheet = "Abonos",
  col_names = TRUE,
  skip = 0)

abonos = as.data.frame(abonos)
file_out3 = paste("Abonos.txt", sep = "" )
directorio_out = "W:\\Ecobici\\Operacion\\"

salida3 = paste(directorio_out, file_out3, sep = "")
fwrite(abonos, 
       salida3, 
       append = FALSE, 
       row.names = FALSE, 
       col.names = TRUE,
       na = 0,
       sep = "\t")


#----------------GUARDO LA TABLITA DE ROBADAS------------------------------
# Como en el paso anterior tengo skip = 1, es el nro de fila -1
robos = read_xlsx(
  nombre_archivo,
  sheet = "Robos",
  col_names = FALSE,
  skip = 18)

fila = which(robos$...1 == "Robadas Vigentes")

robadas_vigentes = as.numeric(robos[fila+1,1])
robadas_historicas = as.numeric(robos[fila+1,3])
recuperadas_vigentes = as.numeric(robos[fila+3,1])
recuperadas_historicas = as.numeric(robos[fila+3,3])
robadas_neto_vigente = as.numeric(robos[fila+5,1])
robadas_neto_historico = as.numeric(robos[fila+5,3])

tablita = rbind(robadas_vigentes, robadas_historicas, recuperadas_vigentes, recuperadas_historicas,
                robadas_neto_vigente, robadas_neto_historico)
tablita = as.data.frame(tablita)
colnames(tablita) = c("Valor")

file_out2 = paste("Tablita.txt", sep = "")
directorio_out = "W:\\Ecobici\\Operacion\\"
salida2 = paste(directorio_out, file_out2, sep = "")

fwrite(tablita, 
       salida2, 
       append = FALSE, 
       row.names = TRUE, 
       col.names = TRUE,
       na = 0,
       sep = "\t")
#---------------------------------------------------------------------------

base$...1 = NULL
base$...2 = NULL
#base$...4 = NULL

# Leo el encabezado para usar como nombre de columnas
header = base[1,]

nro_col = ncol(header)
columnas = rep(NA, nro_col)

for (i in 1: nro_col){
  columnas[i] = as.numeric(header[1,i])
}

columnas[1] = "Columna_Inicial"
colnames(base) = columnas

filas = nrow(base)

# Elimino filas vacías y aquellas que sobran al final
base$largo = str_length(base$Columna_Inicial)
base = base[which(base$largo > 1),]

# Veo en el google sheets que la última fila a tener en cuenta (antes de la tablita) es la que dice "INGRESO"
base$ultima_fila = ifelse(base$Columna_Inicial == "COMPRA MERCADO PAGO - ERROR-", 1,0 )
ult_fila = which(grepl(1, base$ultima_fila))

base$largo = NULL
base$ultima_fila = NULL
base = base[1:ult_fila,]

fechas = colnames(base)
fechas = fechas[2:ncol(base)]
fechas = as.numeric(fechas)
fechas = as.Date(fechas, origin = "1899-12-30")

nombre_columnas = as.character(base[1:ult_fila, 1])

# 20210730 - Agrego una fila adicional con el acumulado de robadas
rownames(base) = NULL
robadas_acum_nls = (base[1,]) #tomo una fila cualquiera
robadas_acum_nls[1,1] = "Robadas_Acumuladas_NLS"
fila = which(base$Columna_Inicial == "Robadas diario")

for (k in (2:ncol(robadas_acum_nls))){
  if (k == 2){
    robadas_acum_nls[1,k] = 0
  }else{
    robadas_acum_nls[1,k] = as.numeric(base[fila, k]) + as.numeric(robadas_acum_nls[1,k-1])
  }
}

base = rbind(base, robadas_acum_nls)

# Importo las referencias de las columnas así divido todo en subtablas
nombre_archivo = "Ref_Columnas_NuevoSheets.xlsx"
directorio = "W:\\Ecobici\\Operacion\\"
path_ = paste(directorio, nombre_archivo, sep = "")
ref_columnas = read_xlsx(path_, sheet = "Ref_Columnas")

row.names(base) = NULL
base = cbind(Index = rownames(base), base)

base = merge(ref_columnas, base, by.x = "Index", by.y = "Index", all.x = TRUE)
base$Columna_Inicial = NULL

# Anular dinamización de columnas
columnas_unpivot = colnames(base)[1:7]
base = melt(base, id = columnas_unpivot)
names(base)[names(base) == 'variable'] = 'Fecha'
names(base)[names(base) == 'value'] = 'Valor'

base$Index = NULL

# Elimino los datos de fechas que son posteriores a hoy
base$Fecha = as.character(base$Fecha)
base$Fecha = as.Date(as.numeric(base$Fecha), origin = "1899-12-30")
hoy = today()
base = base[which(base$Fecha < hoy),]

# Ordeno por tabla, subtabla, nombre_columna y fecha
base = base[order(base$Tabla, base$Sub_Tabla, base$Nombre_OK, base$Fecha),]

row.names(base) = NULL # reseteo el índice
base$Valor = as.numeric(base$Valor) # convierto en valor numérico

# Separo en las distintas tablas
base_separada = split(base, base$Tabla)

estaciones = base_separada[[1]]
facturacion = base_separada[[2]]
patrimonio = base_separada[[3]]
reclamos = base_separada[[4]]
usuarios = base_separada[[5]]
viajes = base_separada[[6]]

# Guardo las tablas
guardar = function(df, nombre_df){
  directorio_out = "W:\\Ecobici\\Operacion\\Tablas_Conteo\\"
  file_out = paste(nombre_df, ".txt", sep = "")
  salida = paste(directorio_out, file_out, sep = "")
  fwrite(df, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")  
}

rm("base_separada", "ref_columnas")
#x = sapply(sapply(ls(), get), is.data.frame)
#lista = names(x)[(x==TRUE)] 
lista = c("estaciones", "facturacion", "patrimonio", "reclamos", "usuarios", "viajes")

for (j in (1:length(lista))){
  df_name = lista[j]
  df = get(df_name)
  # Elimino las columnas Conservar e Historico
  df[,3] = NULL
  df[,5] = NULL
  guardar(df, df_name)
}

# ------------------------------------------------------------------
# Armo el histórico...

directorio = "W:\\Ecobici\\Operacion\\"
setwd(directorio)
file = "Historico_Nuevo.txt"
historico = fread(file, 
             header = TRUE, 
             sep = "\t", 
             #colClasses = clases,
             #nrows = numero_estimado_filas,
             stringsAsFactors = TRUE)
historico$Fecha = as.Date(historico$Fecha, "%d/%m/%Y")

# Saco del historico las columnas que no me sirven
historico$`Fin de vida` = NULL
historico$`Taller (Reparada)` = NULL

# Anular dinamización de columnas
columnas_unpivot = colnames(historico)[12]
historico = melt(historico, id = columnas_unpivot)
names(historico)[names(historico) == 'variable'] = 'Nombre_OK'
names(historico)[names(historico) == 'value'] = 'Valor'


base_historico = base[which(base$Historico == 1),]
# Me quedo solo con las 3 columnas que importan en este caso
base_historico = base_historico[,c(2,7,8)]

orden = colnames(historico)
base_historico = base_historico[, orden]

# Me tengo que fijar el max de robadas_acumulado
max_robos_acum = max(historico[which(historico$Nombre_OK == "Bicicletas_Robadas_Acumulado"),3])

base_historico$Valor = ifelse(base_historico$Nombre_OK == "Robadas_Acumuladas_NLS",
                              base_historico$Valor + max_robos_acum,
                              base_historico$Valor)

base_historico$Nombre_OK = ifelse(base_historico$Nombre_OK == "Robadas_Acumuladas_NLS",
                                  "Bicicletas_Robadas_Acumulado",
                                  base_historico$Nombre_OK)

historico_final = rbind(historico, base_historico)

# Elimino duplicados por fecha
historico_final = historico_final %>% distinct(Fecha, Nombre_OK, .keep_all = TRUE)
colnames(historico_final) = c("Fecha", "Variable", "Valor")

historico_final = historico_final[order(historico_final$Variable, historico_final$Fecha),]

file_out = paste("Operacion_Ecobici_Historico.txt", sep="")
directorio_out = "W:\\Ecobici\\Operacion\\Tablas_Conteo\\"
salida = paste(directorio_out, file_out, sep = "")

fwrite(historico_final, 
       salida, 
       append = FALSE, 
       row.names = FALSE, 
       col.names = TRUE,
       na = 0,
       sep = "\t")



# Guardo el archivo de estaciones

# Busco archivos con el nombre de Operacion Conteo Diario
#id_nomina = drive_get("Status Diario - Conteo")$id
#id_estaciones = "1ND4ArLB46dhltTEUg7UBI-9PQDY40pA2IXbBTvvEk2E"
id_estaciones = "1ND4ArLB46dhltTEUg7UBI-9PQDY40pA2IXbBTvvEk2E"
archivo = as_id(id_estaciones)
nombre_archivo = "Caracteristicas_Estaciones.xlsx"

# Descargo el archivo
directorio = "W:\\Ecobici\\Referencias\\"
path_ = paste(directorio, nombre_archivo, sep = "")

drive_download(
  file = archivo,
  path = path_,
  overwrite = TRUE)


# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
library(googlesheets4)

gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Backup_DataDiaria_Ecobici"

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
