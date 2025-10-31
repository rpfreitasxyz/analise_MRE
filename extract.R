library(tidyverse)
library(glue)
library(fs)
library(furrr)
library(progressr)

# Create destination directory if it doesn't exist
dir_create("dados/zip")

base_uri <- "https://portaldatransparencia.gov.br/download-de-dados/servidores"

# Generate all combinations of year and month
dados_cronologicos <- tidyr::expand_grid(
  ano = 2022:2025,
  mes = 1:12
) |>
  dplyr::mutate(
    mes_formatado = stringr::str_pad(mes, width = 2, side = "left", pad = "0")
  ) |>
  dplyr::mutate(
    arquivos_base = glue::glue("{ano}{mes_formatado}_Servidores_SIAPE"),
    arquivos_url = glue::glue("{base_uri}/{arquivos_base}"),
    destfile = glue::glue("dados/zip/{arquivos_base}.zip")
  )

# Set up parallel processing
plan(multisession)

# Download function that updates the progress bar
download_servidores <- function(url, dest, p) {
  # Using tryCatch to handle potential download errors (e.g., 404 Not Found)
  tryCatch({
    curl::curl_download(url, destfile = dest)
  }, error = function(e) {
    message(paste("Failed to download", url, "-", e$message))
  })
  # Signal a progress update
  p()
}

# Download all files in parallel with a progress bar
with_progress({
  p <- progressor(steps = nrow(dados_cronologicos))
  future_walk2(
    dados_cronologicos$arquivos_url,
    dados_cronologicos$destfile,
    ~ download_servidores(.x, .y, p)
  )
})

print("All downloads attempted.")
