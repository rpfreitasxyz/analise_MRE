# ============================================================================
# Análise de Dados - Remuneração de Diplomatas do MRE
# ============================================================================
# Este script realiza análises sobre os dados de remuneração de diplomatas
# do Ministério das Relações Exteriores (MRE) extraídos pelo script extract.R
# ============================================================================

# --- Carregamento de Pacotes ---
library(dplyr)
library(tidyr)
library(stringr)
library(ggplot2)
library(lubridate)
library(scales)
library(fst)

# --- Função auxiliar para salvar gráficos (evita erro de API mismatch) ---
salvar_grafico <- function(grafico, arquivo, largura = 10, altura = 6) {
  grDevices::png(arquivo, width = largura, height = altura, units = "in", res = 300)
  print(grafico)
  dev.off()
}

# --- Configuração de Tema para Gráficos ---
tema_personalizado <- theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    plot.subtitle = element_text(hjust = 0.5, color = "gray40"),
    legend.position = "bottom",
    panel.grid.minor = element_blank()
  )

theme_set(tema_personalizado)

# Paleta de cores para cargos diplomáticos (hierarquia)
# Cores distintas em gradiente: do mais alto (vermelho escuro) ao mais baixo (amarelo)
# Representa a "escadinha de patentes" da carreira diplomática
cores_cargos <- c(
  "MINISTRO DE PRIMEIRA CLASSE" = "#8B0000",   # Vermelho escuro - topo da carreira
  "MINISTRO DE SEGUNDA CLASSE" = "#CC3300",   # Vermelho alaranjado
  "CONSELHEIRO"                = "#E65C00",   # Laranja
  "PRIMEIRO SECRETARIO"        = "#FF8C00",   # Laranja dourado
  "SEGUNDO SECRETARIO"         = "#FFB347",   # Pêssego
  "TERCEIRO SECRETARIO"        = "#FFDB58"    # Amarelo mostarda - entrada na carreira
)

# --- Carregamento dos Dados ---
cat("Carregando dados...\n")
dados <- fst::read_fst("dados/base_dados_tratados.fst")
cat(paste0("Registros carregados: ", format(nrow(dados), big.mark = ","), "\n"))

# --- Visão Geral dos Dados ---
cat("\n=== VISÃO GERAL DOS DADOS ===\n")
cat(paste0("Período: ", min(dados$periodo_extraido), " a ", max(dados$periodo_extraido), "\n"))
cat(paste0("Total de servidores únicos: ", n_distinct(dados$Id_SERVIDOR), "\n"))
cat(paste0("Total de unidades de lotação: ", n_distinct(dados$UORG_LOTACAO), "\n"))

# --- Explicação da Metodologia ---
cat("\n=== METODOLOGIA DE TRATAMENTO DOS DADOS ===\n")
cat("┌─────────────────────────────────────────────────────────────────────────────┐\n")
cat("│ HIERARQUIA DIPLOMÁTICA (escadinha de patentes):                            │\n")
cat("│   1. Ministro de Primeira Classe (Embaixador) - topo da carreira           │\n")
cat("│   2. Ministro de Segunda Classe                                            │\n")
cat("│   3. Conselheiro                                                           │\n")
cat("│   4. Primeiro Secretário                                                   │\n")
cat("│   5. Segundo Secretário                                                    │\n")
cat("│   6. Terceiro Secretário - entrada na carreira (após concurso)             │\n")
cat("├─────────────────────────────────────────────────────────────────────────────┤\n")
cat("│ TRATAMENTO DA REMUNERAÇÃO:                                                 │\n")
cat("│   - Valores em USD convertidos para BRL usando câmbio do último dia útil   │\n")
cat("│   - Rubricas consideradas para RENDIMENTOS:                                │\n")
cat("│       REMUNERAÇÃO, VENCIMENTO, GRATIFICAÇÃO, ADICIONAL, SUBSÍDIO           │\n")
cat("│   - Rubricas de desconto (IRRF, PSS) são excluídas do cálculo de renda     │\n")
cat("│   - Remuneração MÉDIA = soma dos rendimentos / qtd de rubricas positivas   │\n")
cat("│   - Remuneração TOTAL = soma de TODAS as rubricas positivas do servidor    │\n")
cat("└─────────────────────────────────────────────────────────────────────────────┘\n")

# ============================================================================
# 1. ANÁLISE: Distribuição de Servidores por Cargo
# ============================================================================

contagem_cargos <- dados %>%
  distinct(Id_SERVIDOR, DESCRICAO_CARGO) %>%
  count(DESCRICAO_CARGO, name = "quantidade") %>%
  arrange(desc(quantidade)) %>%
  mutate(DESCRICAO_CARGO = factor(DESCRICAO_CARGO, levels = rev(DESCRICAO_CARGO)))

grafico_cargos <- ggplot(contagem_cargos, aes(x = quantidade, y = DESCRICAO_CARGO, fill = DESCRICAO_CARGO)) +
  geom_col(show.legend = FALSE) +
  geom_text(aes(label = quantidade), hjust = -0.1, size = 3.5) +
  scale_fill_manual(values = cores_cargos) +
  scale_x_continuous(expand = expansion(mult = c(0, 0.15))) +
  labs(
    title = "Distribuição de Diplomatas por Cargo",
    subtitle = "Quantidade de servidores únicos",
    x = "Quantidade de Servidores",
    y = NULL
  )

salvar_grafico(grafico_cargos, "dados/grafico_distribuicao_cargos.png", 10, 6)
cat("\nGráfico salvo: dados/grafico_distribuicao_cargos.png\n")

# ============================================================================
# 2. ANÁLISE: Evolução da Remuneração TOTAL Média por Servidor ao Longo do Tempo
# ============================================================================

# METODOLOGIA: Para cada servidor em cada mês, soma-se TODAS as rubricas positivas
# (rendimentos) e depois calcula-se a média e mediana entre os servidores daquele cargo
cat("\n[Análise 2] Calculando remuneração TOTAL por servidor (soma de todas rubricas positivas)...\n")

remuneracao_por_servidor <- dados %>%
  # Filtra apenas rubricas que são rendimentos (positivos)
  filter(str_detect(nome_cols, "REMUNERA|VENCIMENTO|GRATIFICA|ADICIONAL|SUBSÍDIO|SUBSIDIO|RETRIBUIÇÃO|RETRIBUICAO")) %>%
  filter(valor_cols > 0) %>%
  # Soma todas as rubricas de cada servidor em cada mês
  group_by(periodo_extraido, Id_SERVIDOR, DESCRICAO_CARGO) %>%
  summarise(remuneracao_total_servidor = sum(valor_cols, na.rm = TRUE), .groups = "drop")

# Agora calcula a média entre os servidores de cada cargo
remuneracao_media_cargo <- remuneracao_por_servidor %>%
  group_by(periodo_extraido, DESCRICAO_CARGO) %>%
  summarise(
    remuneracao_media = mean(remuneracao_total_servidor, na.rm = TRUE),
    remuneracao_mediana = median(remuneracao_total_servidor, na.rm = TRUE),
    qtd_servidores = n(),
    .groups = "drop"
  ) %>%
  # Ordena os cargos pela hierarquia
  mutate(DESCRICAO_CARGO = factor(DESCRICAO_CARGO, levels = c(
    "MINISTRO DE PRIMEIRA CLASSE", "MINISTRO DE SEGUNDA CLASSE",
    "CONSELHEIRO", "PRIMEIRO SECRETARIO", "SEGUNDO SECRETARIO", "TERCEIRO SECRETARIO"
  )))

grafico_evolucao <- ggplot(remuneracao_media_cargo, 
                           aes(x = periodo_extraido, y = remuneracao_media, color = DESCRICAO_CARGO)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 0.8, alpha = 0.6) +
  scale_color_manual(values = cores_cargos) +
  scale_y_continuous(labels = label_number(scale = 1/1000, suffix = "k", prefix = "R$ "),
                     breaks = scales::pretty_breaks(n = 8)) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  labs(
    title = "Evolução da Remuneração Total Média por Cargo",
    subtitle = "Soma de todas as rubricas positivas por servidor, média mensal por cargo",
    x = NULL,
    y = "Remuneração Total Média (R$)",
    color = "Hierarquia Diplomática"
  ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.text = element_text(size = 8))

salvar_grafico(grafico_evolucao, "dados/grafico_evolucao_remuneracao.png", 14, 8)
cat("Gráfico salvo: dados/grafico_evolucao_remuneracao.png\n")

# ============================================================================
# 3. ANÁLISE: Top 15 Unidades de Lotação por Número de Diplomatas
# ============================================================================

top_lotacoes <- dados %>%
  filter(periodo_extraido == max(periodo_extraido)) %>%
  distinct(Id_SERVIDOR, UORG_LOTACAO) %>%
  count(UORG_LOTACAO, name = "quantidade") %>%
  slice_max(quantidade, n = 15) %>%
  mutate(UORG_LOTACAO = str_wrap(UORG_LOTACAO, width = 40)) %>%
  mutate(UORG_LOTACAO = factor(UORG_LOTACAO, levels = rev(UORG_LOTACAO)))

grafico_lotacoes <- ggplot(top_lotacoes, aes(x = quantidade, y = UORG_LOTACAO)) +
  geom_col(fill = "#3949ab", alpha = 0.85) +
  geom_text(aes(label = quantidade), hjust = -0.1, size = 3.5) +
  scale_x_continuous(expand = expansion(mult = c(0, 0.1))) +
  labs(
    title = "Top 15 Unidades de Lotação",
    subtitle = paste0("Dados do período mais recente: ", max(dados$periodo_extraido)),
    x = "Quantidade de Diplomatas",
    y = NULL
  )

salvar_grafico(grafico_lotacoes, "dados/grafico_top_lotacoes.png", 12, 8)
cat("Gráfico salvo: dados/grafico_top_lotacoes.png\n")

# ============================================================================
# 4. ANÁLISE: Composição da Remuneração (Rubricas Principais)
# ============================================================================

# Agrupa rubricas similares
composicao_remuneracao <- dados %>%
  filter(valor_cols > 0) %>%
  mutate(rubrica_agrupada = case_when(
    str_detect(nome_cols, "VENCIMENTO|SUBSIDIO|SUBSÍDIO") ~ "Vencimento/Subsídio",
    str_detect(nome_cols, "GRATIFICA") ~ "Gratificações",
    str_detect(nome_cols, "ADICIONAL") ~ "Adicionais",
    str_detect(nome_cols, "AUXÍLIO|AUXILIO") ~ "Auxílios",
    str_detect(nome_cols, "IRRF|IMPOSTO") ~ "IRRF",
    str_detect(nome_cols, "PSS|PREVIDÊN|PREVIDEN") ~ "Previdência",
    str_detect(nome_cols, "EXTERIOR") ~ "Retribuição Exterior",
    str_detect(nome_cols, "FÉRIAS|FERIAS") ~ "Férias/13º",
    TRUE ~ "Outros"
  )) %>%
  group_by(rubrica_agrupada) %>%
  summarise(
    valor_total = sum(valor_cols, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(percentual = valor_total / sum(valor_total) * 100) %>%
  arrange(desc(valor_total))

grafico_composicao <- ggplot(composicao_remuneracao, 
                              aes(x = reorder(rubrica_agrupada, valor_total), y = valor_total, fill = rubrica_agrupada)) +
  geom_col(show.legend = FALSE) +
  geom_text(aes(label = paste0(round(percentual, 1), "%")), hjust = -0.1, size = 3.5) +
  coord_flip() +
  scale_y_continuous(labels = label_number(scale = 1/1e9, suffix = " bi", prefix = "R$ "),
                     expand = expansion(mult = c(0, 0.15))) +
  scale_fill_viridis_d(option = "mako", begin = 0.2, end = 0.8) +
  labs(
    title = "Composição da Remuneração por Tipo de Rubrica",
    subtitle = "Soma total de todos os períodos",
    x = NULL,
    y = "Valor Total (R$ bilhões)"
  )

salvar_grafico(grafico_composicao, "dados/grafico_composicao_remuneracao.png", 10, 6)
cat("Gráfico salvo: dados/grafico_composicao_remuneracao.png\n")

# ============================================================================
# 5. ANÁLISE: Quantidade de Diplomatas ao Longo do Tempo
# ============================================================================

qtd_diplomatas_tempo <- dados %>%
  group_by(periodo_extraido, DESCRICAO_CARGO) %>%
  summarise(qtd_servidores = n_distinct(Id_SERVIDOR), .groups = "drop")

grafico_qtd_tempo <- ggplot(qtd_diplomatas_tempo, 
                            aes(x = periodo_extraido, y = qtd_servidores, fill = DESCRICAO_CARGO)) +
  geom_area(alpha = 0.8) +
  scale_fill_manual(values = cores_cargos) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  labs(
    title = "Evolução do Número de Diplomatas por Cargo",
    subtitle = "Série histórica",
    x = NULL,
    y = "Quantidade de Servidores",
    fill = "Cargo"
  ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

salvar_grafico(grafico_qtd_tempo, "dados/grafico_evolucao_quantidade.png", 12, 7)
cat("Gráfico salvo: dados/grafico_evolucao_quantidade.png\n")

# ============================================================================
# 6. ANÁLISE: Estatísticas Descritivas por Cargo (Período mais recente)
# ============================================================================

periodo_recente <- max(dados$periodo_extraido)

estatisticas_cargo <- dados %>%
  filter(periodo_extraido == periodo_recente) %>%
  filter(str_detect(nome_cols, "REMUNERA|VENCIMENTO|GRATIFICA|ADICIONAL|SUBSÍDIO|SUBSIDIO")) %>%
  group_by(Id_SERVIDOR, nome_servidor, DESCRICAO_CARGO) %>%
  summarise(remuneracao_total = sum(valor_cols, na.rm = TRUE), .groups = "drop") %>%
  group_by(DESCRICAO_CARGO) %>%
  summarise(
    qtd_servidores = n(),
    media = mean(remuneracao_total),
    mediana = median(remuneracao_total),
    desvio_padrao = sd(remuneracao_total),
    minimo = min(remuneracao_total),
    maximo = max(remuneracao_total),
    .groups = "drop"
  ) %>%
  arrange(desc(media))

cat("\n=== ESTATÍSTICAS DESCRITIVAS - PERÍODO:", as.character(periodo_recente), "===\n\n")
print(estatisticas_cargo)

# Exporta estatísticas
write.csv(estatisticas_cargo, "dados/estatisticas_por_cargo.csv", row.names = FALSE)
cat("\nTabela salva: dados/estatisticas_por_cargo.csv\n")

# ============================================================================
# 7. ANÁLISE: Boxplot de Remuneração por Cargo
# ============================================================================

remuneracao_individual <- dados %>%
  filter(periodo_extraido == periodo_recente) %>%
  filter(str_detect(nome_cols, "REMUNERA|VENCIMENTO|GRATIFICA|ADICIONAL|SUBSÍDIO|SUBSIDIO")) %>%
  group_by(Id_SERVIDOR, DESCRICAO_CARGO) %>%
  summarise(remuneracao_total = sum(valor_cols, na.rm = TRUE), .groups = "drop")

# Ordena cargos pela hierarquia
ordem_cargos <- c("MINISTRO DE PRIMEIRA CLASSE", "MINISTRO DE SEGUNDA CLASSE", 
                  "CONSELHEIRO", "PRIMEIRO SECRETARIO", "SEGUNDO SECRETARIO", "TERCEIRO SECRETARIO")

remuneracao_individual <- remuneracao_individual %>%
  mutate(DESCRICAO_CARGO = factor(DESCRICAO_CARGO, levels = ordem_cargos))

grafico_boxplot <- ggplot(remuneracao_individual, 
                          aes(x = DESCRICAO_CARGO, y = remuneracao_total, fill = DESCRICAO_CARGO)) +
  geom_boxplot(alpha = 0.8, outlier.alpha = 0.5) +
  scale_fill_manual(values = cores_cargos) +
  scale_y_continuous(labels = label_number(scale = 1/1000, suffix = "k", prefix = "R$ ")) +
  coord_flip() +
  labs(
    title = "Distribuição da Remuneração por Cargo",
    subtitle = paste0("Período: ", periodo_recente),
    x = NULL,
    y = "Remuneração Total (R$)"
  ) +
  theme(legend.position = "none")

salvar_grafico(grafico_boxplot, "dados/grafico_boxplot_remuneracao.png", 10, 6)
cat("Gráfico salvo: dados/grafico_boxplot_remuneracao.png\n")

# ============================================================================
# 8. RESUMO FINAL
# ============================================================================

cat("\n")
cat("============================================================\n")
cat("                    RESUMO DA ANÁLISE                       \n")
cat("============================================================\n")
cat(paste0("Período analisado: ", min(dados$periodo_extraido), " a ", max(dados$periodo_extraido), "\n"))
cat(paste0("Total de registros: ", format(nrow(dados), big.mark = ","), "\n"))
cat(paste0("Diplomatas únicos: ", n_distinct(dados$Id_SERVIDOR), "\n"))
cat(paste0("Unidades de lotação: ", n_distinct(dados$UORG_LOTACAO), "\n"))
cat("\n")
cat("Arquivos gerados:\n")
cat("  - dados/grafico_distribuicao_cargos.png\n")
cat("  - dados/grafico_evolucao_remuneracao.png\n")
cat("  - dados/grafico_top_lotacoes.png\n")
cat("  - dados/grafico_composicao_remuneracao.png\n")
cat("  - dados/grafico_evolucao_quantidade.png\n")
cat("  - dados/grafico_boxplot_remuneracao.png\n")
cat("  - dados/estatisticas_por_cargo.csv\n")
cat("============================================================\n")
