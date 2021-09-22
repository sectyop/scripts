# Package names
packages <- c("dplyr", "lubridate", "data.table", "readxl", "googlesheets4")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))



file = "Nomina_CAT_Actual.txt"
directorio = "W:\\Agentes Viales\\Nomina\\"
file = paste(directorio, file, sep = "")

nomina = fread(file,
               encoding = 'UTF-8')

columnas_preservar = c('Ficha', 'Apellido_', 'Nombre', 'Fecha de nacimiento', 'Fecha de ingreso al GCBA', 
                       'Sexo', 'CUIT', 'Numero_CAT','DNI', 'Tipo de inscripcion', 'Cargo', 'Base', 'Estado Civil', 'Estado', 
                       'Email gobierno', 'Email', 'Telefono ht', 'Funcion', 'Area', 'Turno', 'Tipo de contrato')

nomina = nomina %>% select(all_of(columnas_preservar))

# Ahora ya tengo los nros_cat en la nómina y todo esto no hace falta.
# Abro archivo con los nros_cat
# file = "DGCATRA-Listado Nro CAT.xlsx"
# directorio = "W:\\Agentes Viales\\Nomina\\"
# file = paste(directorio, file, sep = "")
# 
# activos = read_xlsx(
#   file,
#   sheet = "CAT ACTIVOS",
#   col_names = TRUE,
#   skip = 0,
#   col_types = "text")
# 
# bajas = read_xlsx(
#   file,
#   sheet = "CAT BAJAS",
#   col_names = TRUE,
#   skip = 0,
#   col_types = "text")
# 
# nros_cat = dplyr::bind_rows(activos, bajas)
# 
# columnas_nros_cat = c('CUIT', 'CAT')
# nros_cat = nros_cat %>% select(all_of(columnas_nros_cat))
# ## elimino guiones de cuits con formato 00-00000000-0
# nros_cat$CUIT = gsub("-", "", nros_cat$CUIT)


#nomina = merge(nomina, nros_cat, by.x = "CUIT", by.y = "CUIT", all.x = TRUE)

# Hago los filtros necesarios

# 1) Estado = ACTIVO
# Acá necesito ver el estado corregido del archivo W:\Agentes Viales\Nomina\Nomina 1.2.xlsx
# Cambio sugerido por JIR el 17/8/2021
file = "Nomina 1.2.xlsx"
directorio = "W:\\Agentes Viales\\Nomina\\"
file = paste(directorio, file, sep = "")

nomina_12 = read_xlsx(
  file,
  sheet = "Datos CAT",
  col_names = TRUE,
  skip = 0,
  col_types = "text")

estados_corregidos = as.data.frame(cbind(nomina_12$`CUIT Correg`, nomina_12$`Estado Corregido`))
colnames(estados_corregidos) = c("CUIT", "Estado_Corregido")
nomina = merge(nomina, estados_corregidos, by.x = "CUIT", by.y = "CUIT", all.x = TRUE)

nomina = nomina[which(nomina$Estado_Corregido == "ACTIVO"),]

# 2) Funcion = Operativo
# Acá necesito ver el Tipo Función del archivo W:\Agentes Viales\Nomina\Nomina 1.2.xlsx
# Cambio sugerido por JIR el 17/8/2021
tipo_funcion = as.data.frame(cbind(nomina_12$`CUIT Correg`, nomina_12$`Tipo Función`))
colnames(tipo_funcion) = c("CUIT", "tipo_funcion")
nomina = merge(nomina, tipo_funcion, by.x = "CUIT", by.y = "CUIT", all.x = TRUE)

nomina = nomina[which(nomina$tipo_funcion == "Operaciones"),]

# 3) Bases a incluir
bases_incluir = c('BRD Sarmiento', 'BRD Tacuarí', 'Cochabamba', 'Dakota', 'Las Heras', 'Piedras', 'Luro', 'Vedia')
nomina = nomina[which(nomina$Base %in% bases_incluir),]

# Ajusto valores

# 1) Sexo
nomina$Sexo = ifelse(nomina$Sexo != "M" & nomina$Sexo != "F", "OTROS", nomina$Sexo)

# 2) Nacionalidad
nomina$DNI = as.integer(nomina$DNI)
nomina$NACIONALIDAD = ifelse(nomina$DNI < 90000000, "Argentino/a", NA)

# 3) Tipo de contrato / Tipo de inscripcion
nomina$TIPOCONTRATO = ifelse(nomina$`Tipo de inscripcion` == "-1" | nomina$`Tipo de inscripcion` == "",
                             nomina$`Tipo de contrato`,
                             nomina$`Tipo de inscripcion`)

# 4) Estado
nomina$Estado = "Servicio Efectivo - Efectivo"

# 5) Mail
nomina$MAIL = ifelse(nchar(nomina$`Email gobierno`) > 3,
                     nomina$`Email gobierno`,
                     nomina$Email)

nomina$TIPO_MAIL = ifelse(nomina$MAIL == nomina$`Email gobierno`,
                          "INSTITUCIONAL",
                          "PARTICULAR")
# 6) Telefono
nomina$`Telefono ht` = as.numeric(nomina$`Telefono ht`)
nomina$TELEFONO = ifelse(is.na(nomina$`Telefono ht`),
                         NA,
                         nomina$`Telefono ht`)
nomina$TIPO_TELEFONO = ifelse(nomina$TELEFONO > 0,
                              "ASIGNADO",
                              NA)
# 7) Estado civil
estado_civil_cat = c('CASADO', 'CONCUBINATO', 'CONCUVINO', 'DIVORCIADO', 'SEPARADO DE HECHO', 'SOLTERO', 'UNION CIVIL', 'VIUDO')
estado_civil_sirhu = c('Casado/a', 'Conviviente', 'Conviviente', 'Divorciado/a', 'Separación de hecho', 'Soltero/a', 'Conviviente', 'Viudo/a')
df_estado_civil = data.frame("Estado_Civil_CAT" = estado_civil_cat, "Estado_CIVIL_SIRHU" = estado_civil_sirhu)

nomina = merge(nomina, df_estado_civil, by.x = "Estado Civil", by.y = "Estado_Civil_CAT", all.x = TRUE)
nomina$ESTADO_CIVIL = nomina$Estado_CIVIL_SIRHU

# 8) Cargos

cargos_cat = c('Coordinador', 'Coordinador Gral.', 'Gerente', 'Jefe de Base', 'Jefe de Departamento', 'Subgerente', 'Supervisor')
cargos_sirhu = c('Coordinador', 'Coordinador', 'Gerente ', 'Jefe de Base', 'Jefe de Departamento', 'Subgerente ', 'Supervisor')
df_cargos = data.frame("Cargos_CAT" = cargos_cat, "Cargos_SIRHU" = cargos_sirhu)

nomina = merge(nomina, df_cargos, by.x = "Cargo", by.y = "Cargos_CAT", all.x = TRUE)
nomina$CARGO = nomina$Cargos_SIRHU

## si el cargo está vacío, es agente de tránsito
nomina$CARGO = ifelse(is.na(nomina$CARGO),
                      "Agente",
                      nomina$CARGO)
## Selecciono únicamente agentes, coordinadores y supervisores
cargos_seleccion = c("Agente", "Coordinador", "Supervisor")
nomina = nomina[which(nomina$CARGO %in% cargos_seleccion),]

# Renombro las columnas que quedan
names(nomina)[names(nomina) == 'Numero_CAT'] = 'LEGAJO'
names(nomina)[names(nomina) == 'Apellido_'] = 'APELLIDO'
names(nomina)[names(nomina) == 'Nombre'] = 'NOMBRE'
names(nomina)[names(nomina) == 'Fecha de nacimiento'] = 'FECHA_NAC'
names(nomina)[names(nomina) == 'Fecha de ingreso al GCBA'] = 'FECHA_INGRESO'
names(nomina)[names(nomina) == 'Sexo'] = 'SEXO'
names(nomina)[names(nomina) == 'CUIT'] = 'CUIL'
names(nomina)[names(nomina) == 'DNI'] = 'NRO_DOC'
names(nomina)[names(nomina) == 'Base'] = 'DESTINO'
names(nomina)[names(nomina) == 'Estado_Corregido'] = 'RELACION DE EMPLEO'
names(nomina)[names(nomina) == 'tipo_funcion'] = 'TIPO_AREA'
names(nomina)[names(nomina) == 'Area'] = 'AREA'
names(nomina)[names(nomina) == 'Turno'] = 'TURNO'

# Elijo las columnas que quedan
columnas_preservar = c('LEGAJO','APELLIDO','NOMBRE','FECHA_NAC','FECHA_INGRESO','SEXO','NACIONALIDAD','CUIL','NRO_DOC',
                       'TIPOCONTRATO','CARGO','DESTINO','ESTADO_CIVIL','RELACION DE EMPLEO','TIPO_MAIL','MAIL',
                       'TIPO_TELEFONO','TELEFONO','TIPO_AREA','AREA','TURNO')

nomina = nomina %>% select(all_of(columnas_preservar))


# Guardo el archivo...
file_out = "Nomina_CAT_SIRHU.csv"
directorio_out = directorio
salida = paste(directorio_out, file_out, sep = "")

# fwrite(nomina, 
#        salida, 
#        append = FALSE, 
#        row.names = FALSE, 
#        col.names = TRUE,
#        sep = "\t")

write.csv(nomina,
          salida,
          row.names = FALSE)

# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
gs4_auth(email = "datos.ssgm@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Genera_nomina_sirhu"

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
