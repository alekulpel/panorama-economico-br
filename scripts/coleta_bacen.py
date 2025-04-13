# scripts/coleta_bacen.py

print("🚀 Iniciando script de coleta de dados do BACEN SGS...")

import pandas as pd
from bcb import sgs # Importa a funcionalidade SGS da biblioteca python-bcb
import os
import logging
from datetime import datetime # Para usar a data atual como padrão

# Configuração do Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Códigos das Séries no SGS/BCB ---
# É uma boa prática verificar os códigos e suas descrições no site do BCB
# https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries
CODIGOS_BCB = {
    # Nome descritivo: código SGS
    #'selic_meta': 432,        # Taxa de juros - Meta Selic definida pelo Copom (% a.a.) - Diária (mas muda menos freq.)
    #'selic_diaria': 11,       # Taxa de juros - Selic acumulada no mês (% a.m.) - Diária - CUIDADO: essa é mensal acumulada, talvez 1178 seja melhor (efetiva diária)
    #'selic_efetiva_diaria': 1178, # Taxa de juros - Selic efetiva (% a.a.) - Base 252 - Diária
    #'ipca_mensal': 433,       # Índice nacional de preços ao consumidor-amplo (IPCA) - Var. % mensal - Mensal
    'cambio_usd_venda': 1,    # Taxa de Câmbio - Livre - Dólar americano (venda) - Boletim diário PTAX - Diária
    # Adicione outros códigos se precisar (ex: IGP-M, Produção Industrial do IBGE via BCB, etc.)
}

# --- Parâmetros ---
# Use uma data de início razoável para o seu projeto
DATA_INICIO = '2002-01-01' # Exemplo: início de 2002
# Pode usar a data atual como data fim padrão
# DATA_FIM = datetime.today().strftime('%Y-%m-%d') # Descomente se quiser limitar a data fim

# Diretório e nome do arquivo de saída (relativo à pasta 'scripts')
output_dir = "/home/akulpel/Projetos/panorama-economico-br/data/raw"
output_file = os.path.join(output_dir, "indicadores_bacen_bruto.csv")

def coleta_dados_bacen(codigos, data_inicio, filename):
    """
    Coleta séries temporais do SGS/BCB usando a biblioteca python-bcb
    e salva em um arquivo CSV.
    """
    codigos_numericos = list(codigos.values()) # A função sgs.get espera uma lista de números
    nomes_descritivos = list(codigos.keys())   # Usaremos para logging

    logging.info(f"Iniciando coleta do BCB SGS para os códigos: {codigos_numericos} ({', '.join(nomes_descritivos)})")
    logging.info(f"Período solicitado: de {data_inicio} até hoje.") # Ajuste se usar DATA_FIM

    try:
        # A mágica acontece aqui: sgs.get busca todas as séries de uma vez
        # O resultado é um DataFrame com a data como índice e cada série como uma coluna
        df_bacen = sgs.get(codigos, start=data_inicio) # Pode adicionar end=DATA_FIM se definiu

        if df_bacen.empty:
            logging.warning("⚠️ O BCB SGS não retornou dados para os códigos/período especificados.")
            return

        # A biblioteca já nomeia as colunas com os nomes descritivos que passamos no dict!
        logging.info(f"📊 Dados do BCB coletados com sucesso. Shape: {df_bacen.shape}")
        logging.info(f"Colunas: {df_bacen.columns.tolist()}") # Verificar se os nomes estão corretos
        logging.info(f"Período dos dados: {df_bacen.index.min().strftime('%Y-%m-%d')} a {df_bacen.index.max().strftime('%Y-%m-%d')}")
        logging.info(f"Valores ausentes por coluna:\n{df_bacen.isnull().sum()}")

        # Garantir que o diretório exista
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        # Salvar em CSV
        # O índice (data) será salvo como a primeira coluna por padrão
        df_bacen.to_csv(filename, encoding='utf-8')
        logging.info(f"✅ Dados brutos do BCB salvos em: {filename}")

    except Exception as e:
        logging.error(f"❌ Erro ao coletar dados do BCB: {e}", exc_info=True)

# --- Execução ---
if __name__ == "__main__":
    # Verifica se o diretório de saída existe antes de pausar (opcional)
    print(f"Os dados do BACEN serão salvos em: {os.path.abspath(output_file)}") # Mostra caminho absoluto
    # input("Pressione Enter para continuar...") # Pausa opcional

    coleta_dados_bacen(CODIGOS_BCB, DATA_INICIO, output_file)
    print("✅ Script de coleta do BCB finalizado.")