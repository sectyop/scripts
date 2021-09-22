# 23/08/2021
# ----------------------------------------------------------------------------------------------------------------
# JIR quiere una salida de nómina CAT en donde figuren, ademas, valores promedio de presentismo por agente por año
# Tomamos el archivo "Data_Presentismo.txt" y lo procesamos para realizar dicho cálculo
# ----------------------------------------------------------------------------------------------------------------
  
# Limpio la memoria
rm(list=ls())
gc()
Sys.time()

# Instalación condicional de paquetes

# Package names
packages <- c("dplyr", "lubridate", "data.table", "readxl", "reshape", "googlesheets4")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

# Abro el archivo
file = "Data_Presentismo.txt"
directorio = "W:\\Agentes Viales\\Nomina\\"
file = paste(directorio, file, sep = "")

data = fread(file)
#data_bck = data

# Abro el archivo de referencia de códigos
file_ref = "Ref_Codigos.xlsx"
directorio = "W:\\Agentes Viales\\Ref\\"
file_ref = paste(directorio, file_ref, sep = "")

ref_codigos = read_xlsx(
  file_ref,
  sheet = "Códigos",
  col_names = TRUE,
  .name_repair = "unique")

ref_codigos$Tipo_Dia = ifelse(ref_codigos$Tipo != "No Laborable",
                              "Laborable",
                              ref_codigos$Tipo)
ref_codigos_short = ref_codigos %>% select(CODIGO, Ausente_Presente, Tipo_Dia)
ref_codigos_short = unique(ref_codigos_short)

# Agrego el campo de año
data$anio = year(data$fecha)

# Traigo la info de los codigos
data = merge(data, ref_codigos_short, by.x = "codigo", by.y = "CODIGO", all.x = TRUE)

# Empiezo a armar las formulas
data$dia_laborable = ifelse(is.na(data$Ausente_Presente) | data$Tipo_Dia == "Laborable" | data$Ausente_Presente == "",
                            1,
                            0)
data$presente = ifelse(data$Ausente_Presente == "Presente" & data$dia_laborable == 1,
                       1,
                       0)

# Hago las agrupaciones por año
data = data %>%
  group_by(anio, id_agente, cuit) %>%
  dplyr::summarise(dias_presente = sum(presente), dias_laborables = sum(dia_laborable))

# Calculo el porcentaje
data$pct_presente = ifelse(data$dias_laborables > 0, 
                           data$dias_presente / data$dias_laborables,
                           NA)

data$dias_presente = NULL
data$dias_laborables = NULL
data = as.data.frame(data)

# Dinamizo las columnas de años

# Me quedo los ultimos 3 años
anio_max = max(data$anio)
data = data[which(data$anio > anio_max - 3),]

setDT(data)
data = dcast(data, id_agente + cuit ~ anio, fun.aggregate = sum, value.var='pct_presente') 

columnas = c("id_agente", "cuit", paste("presentismo_",anio_max-2, sep= ""), paste("presentismo_",anio_max-1, sep = ""),paste("presentismo_",anio_max, sep =""))
colnames(data) = columnas

# Guardo el archivo de salida
file_out = "Data_Presentismo_3yr.txt"
directorio_out = "W:\\Agentes Viales\\Nomina\\Bajada_Nomina_Adicional\\"
salida = paste(directorio_out, file_out, sep = "")

fwrite(data, 
       salida, 
       append = FALSE, 
       row.names = FALSE, 
       col.names = TRUE,
       #na = 0,
       sep = "\t")


# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
# Lo guardo en un google sheets también
gs4_auth(email = "datos.ssgm@gmail.com")
df_hora_guardado = as.data.frame(Sys.time() - hours(3))

id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"

colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Trabaja_Presentismo"

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
