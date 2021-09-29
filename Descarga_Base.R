# Limpio la memoria
rm(list=ls())
gc()

library(data.table)
library(lubridate)

# Leo el archivo desde Google Sheets
file = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQRzkRL43Hz0GdP4Lpml4eDpv1dZQyD8EO00hinkw8BqJ_Ho1WpUZsANU77AhX7HcNgJCObgq5V-C-v/pub?gid=2021547255&single=true&output=csv"

base = read.csv(file,
                encoding = 'UTF-8',
                skip = 2,
                stringsAsFactors = FALSE,
                header = FALSE)


nombre_salida = "Base_Tablero.csv"
directorio_out = "W:\\COVID\\Tablero\\"
salida = paste(directorio_out, nombre_salida, sep = "")

fwrite(base, salida, append = FALSE, row.names = FALSE, col.names = TRUE, sep = ";")

# Escribo status ejecución en hoja "Status Datos SSGM"
# --------------------------------------------------------------------------------
library(googlesheets4)

gs4_auth(email = "nacho.ls@gmail.com")
id_status = "1BwZjmRPRaFahxI8eWwU6EXAqsSUj7OU2_xWe6KT9u2Y"
df_hora_guardado = as.data.frame(Sys.time() - hours(3))
colnames(df_hora_guardado) = "FechaHoraActual"

codigo_r = "Descarga_Base"

status = read_sheet(id_status)
fila = match(codigo_r, status$Script) + 1

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

