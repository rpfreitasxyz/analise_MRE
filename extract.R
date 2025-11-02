library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(glue)
library(fs)
library(progress)
library(progressr)
library(tidyverse)

#### EXTRACAO WEB ####
# Create destination directory if it doesn't exist
dir_create("dados/zip")

base_uri <- "https://portaldatransparencia.gov.br/download-de-dados/servidores"

# Get current year and month
current_date <- Sys.Date()
current_year <- as.integer(format(current_date, "%Y"))
current_month <- as.integer(format(current_date, "%m"))

# Generate all combinations of year and month
dados_cronologicos <- tidyr::expand_grid(
  ano = 2013:current_year,
  mes = 1:12
) |>
  # Filter out future dates
  dplyr::filter(!(ano == current_year & mes > current_month)) |>
  dplyr::mutate(
    mes_formatado = stringr::str_pad(mes, width = 2, side = "left", pad = "0")
  ) |>
  dplyr::mutate(
    arquivos_base = glue::glue("{ano}{mes_formatado}_Servidores_SIAPE"),
    arquivos_url = glue::glue("{base_uri}/{arquivos_base}"),
    destfile = glue::glue("dados/zip/{arquivos_base}.zip")
  )

# Get list of already downloaded files
downloaded_files <- fs::dir_ls("dados/zip", glob = "*.zip") |>
  fs::path_file() |>
  fs::path_ext_remove()

# Find the most recent downloaded file to re-download it
most_recent_file <- max(downloaded_files)

# Filter out already downloaded files, but keep the most recent one for re-download
dados_cronologicos_filtrados <- dados_cronologicos |>
  dplyr::filter(!arquivos_base %in% downloaded_files | arquivos_base == most_recent_file)

# Initialize progress bar
pb <- progress_bar$new(
  format = "  downloading [:bar] :percent in :elapsed",
  total = nrow(dados_cronologicos_filtrados), clear = FALSE, width = 60)

# Download function that updates the progress bar
download_servidores <- function(url, dest) {
  # Using tryCatch to handle potential download errors (e.g., 404 Not Found)
  tryCatch({
    # quiet = TRUE to not interfere with the progress bar
    curl::curl_download(url, destfile = dest, quiet = TRUE)
  }, error = function(e) {
    # We still want to see errors
    message(paste("Failed to download", url, "-", e$message))
  })
  # Signal a progress update
  pb$tick()
}

# Download all files sequentially
purrr::walk2(
  dados_cronologicos_filtrados$arquivos_url,
  dados_cronologicos_filtrados$destfile,
  download_servidores
)

# --- Unzip files ---

# Create destination directory if it doesn't exist
dir_create("dados/csv")

# Get list of all downloaded zip files
zip_files <- fs::dir_ls("dados/zip", glob = "*.zip")

# Function to unzip a single file
unzip_file <- function(file) {
  # Define the destination directory for the unzipped files
  dest_dir <- fs::path("dados", "csv", fs::path_ext_remove(fs::path_file(file)))

  # Create the directory
  fs::dir_create(dest_dir)

  # Unzip the file
  utils::unzip(file, exdir = dest_dir)
}

# Unzip all files
purrr::walk(zip_files, unzip_file)

rm(list = ls())
gc()

#### EXTRACAO BASAL ####
path_dados_bruto_max <- list.files("dados/csv") %>%
  enframe(name = NULL) %>%
  mutate(periodo_extraido = str_extract(value, "\\d{6}")) %>%
  { . ->> path_dados_bruto } %>%
  filter(max(value) == value)

dados_cadastro_bruto <- data.table::fread(paste0("D:/R/projects/analise_MRE/dados/csv/", 
                                         path_dados_bruto_max$value, "/", 
                                         path_dados_bruto_max$periodo_extraido, "_Cadastro.csv"), encoding = "Latin-1")

dados_cadastro_tratado <- dados_cadastro_bruto %>%
  filter(ORG_LOTACAO == "Ministério das Relações Exteriores",
         DESCRICAO_CARGO %in% c("MINISTRO DE PRIMEIRA CLASSE",
                                "MINISTRO DE SEGUNDA CLASSE",
                                "CONSELHEIRO",
                                "PRIMEIRO SECRETARIO",
                                "SEGUNDO SECRETARIO",
                                "TERCEIRO SECRETARIO"))

dados_remuneracao_bruto <- path_dados_bruto %>%
  mutate(full_path = paste0("D:/R/projects/analise_MRE/dados/csv/", 
                            value, "/", 
                            periodo_extraido, "_Remuneracao.csv")) %>%
  mutate(dados = map(.x = full_path,
                     .f = data.table::fread,
                     quote = "",
                     colClasses = c("character"),
                     encoding = "Latin-1"))

dados_remuneracao_tratado <- dados_remuneracao_bruto %>%
  slice(1) %>%
  unnest(cols = dados)

