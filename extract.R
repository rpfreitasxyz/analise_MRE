library(tidyverse)

base_uri <- "https://portaldatransparencia.gov.br/download-de-dados/servidores"

arquivos_base <- paste0(2025, "02", "_Servidores_SIAPE")

arquivos_url <- paste0(base_uri, "/",
                   arquivos_base)

curl::curl_download(arquivos_url, destfile = paste0("dados/zip/", arquivos_base, ".zip"))