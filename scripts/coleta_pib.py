# scripts/coleta_contas_nacionais.py (ou nome que preferir)

print("🚀 Iniciando script de coleta de dados das Contas Nacionais Trimestrais (SIDRA Tabela 6613)...")

import requests
import pandas as pd
import os
import logging

# Configuração do Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Parâmetros da Consulta ---
TABLE_ID = "6613"
GEO_LEVEL = "n1"
GEO_CODE = "1" # Brasil
VARIABLES = "all"
PERIODS = "all"
CLASSIFICATION = "c11255"
CATEGORIES = "all"
FORMAT_CODE = "v9319%202"

api_url = f"https://apisidra.ibge.gov.br/values/t/{TABLE_ID}/{GEO_LEVEL}/{GEO_CODE}/v/{VARIABLES}/p/{PERIODS}/{CLASSIFICATION}/{CATEGORIES}/d/{FORMAT_CODE}"

output_dir = "/home/akulpel/Projetos/panorama-economico-br/data/raw"
output_file = os.path.join(output_dir, "cnt_6613_setorial_dessazonalizado_bruto.csv")

def coleta_dados_sidra(url, filename):
    logging.info(f"Iniciando coleta da URL: {url}")
    try:
        response = requests.get(url, verify=True, timeout=90)
        response.raise_for_status()
        logging.info(f"✅ Conexão bem-sucedida! Status: {response.status_code}")
        dados_json = response.json()

        if not dados_json or len(dados_json) <= 1:
            logging.warning("⚠️ A API retornou dados vazios ou apenas o cabeçalho.")
            return

        logging.info(f"🔍 Exemplo de dado retornado (primeiro registro): {dados_json[1]}")

        header_map = dados_json[0]
        internal_codes = list(dados_json[1].keys())
        colunas_legiveis = [header_map.get(code, code) for code in internal_codes]
        dados_limpos = [list(item.values()) for item in dados_json[1:]]
        df = pd.DataFrame(dados_limpos, columns=colunas_legiveis)

        logging.info(f"📊 DataFrame criado com {df.shape[0]} linhas e {df.shape[1]} colunas.")
        logging.info(f"Colunas originais: {df.columns.tolist()}")

        # --- Limpeza Básica (AJUSTADA para incluir todas as colunas identificadas) ---
        rename_map = {
            'Nível Territorial (Código)': 'nivel_territorial_cod', # Adicionado
            'Nível Territorial': 'nivel_territorial_nome',     # Adicionado
            'Unidade de Medida (Código)': 'unidade_medida_cod', # Adicionado
            'Unidade de Medida': 'unidade_medida_nome',      # Adicionado
            'Valor': 'valor',
            'Trimestre (Código)': 'trimestre_codigo',
            'Trimestre': 'trimestre_nome',
            'Brasil (Código)': 'geo_cod', # Confirmado que existe
            'Brasil': 'geo_nome',       # Confirmado que existe
            'Setores e subsetores (Código)': 'setor_cod',
            'Setores e subsetores': 'setor_nome',
            'Variável (Código)': 'variavel_cod',
            'Variável': 'variavel_nome'
        }
        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
        logging.info(f"Colunas após renomear: {df.columns.tolist()}")

        # Converter 'valor' para numérico
        if 'valor' in df.columns:
            df['valor'] = df['valor'].replace({'...': pd.NA, '-': pd.NA, 'X': pd.NA}, regex=False)
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
            logging.info("Coluna 'valor' convertida para numérico.")

        # Tratar período trimestral
        if 'trimestre_codigo' in df.columns:
            try:
                df['ano'] = df['trimestre_codigo'].str[:4].astype(int)
                df['trimestre_num'] = df['trimestre_codigo'].str[4:].astype(int)
                df['periodo_trimestral'] = pd.PeriodIndex(year=df['ano'], quarter=df['trimestre_num'], freq='Q')
                logging.info("Colunas 'ano', 'trimestre_num', 'periodo_trimestral' criadas.")

                # --- Opcional: Remover colunas redundantes ---
                # Após criar 'periodo_trimestral', as colunas usadas para criá-la
                # podem não ser mais necessárias para a análise.
                cols_to_drop = ['trimestre_codigo', 'trimestre_nome', 'ano', 'trimestre_num']
                # Remove apenas as colunas que existem no DataFrame
                cols_existentes_para_dropar = [col for col in cols_to_drop if col in df.columns]
                if cols_existentes_para_dropar:
                     df.drop(columns=cols_existentes_para_dropar, inplace=True)
                     logging.info(f"Colunas redundantes removidas: {cols_existentes_para_dropar}")

            except Exception as e:
                logging.warning(f"Não foi possível converter 'trimestre_codigo' ou remover colunas: {e}")


        # Garantir que o diretório de saída exista
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(output_file, index=False, encoding='utf-8')
        logging.info(f"✅ Dados brutos (com limpeza básica) salvos em: {output_file}")

    except requests.exceptions.SSLError as e:
         logging.error(f"❌ Erro de SSL: {e}. Verifique certificados ou tente 'verify=False'.")
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Erro de conexão com a API: {e}")
    except ValueError as e:
        response_text = "N/A (response não definido)"
        try: response_text = response.text[:500]
        except NameError: pass
        logging.error(f"❌ Erro ao processar JSON da API: {e} - Conteúdo inicial: {response_text}")
    except Exception as e:
        logging.error(f"❌ Erro inesperado durante a coleta: {e}", exc_info=True)

# --- Execução ---
if __name__ == "__main__":
    print(f"URL da API que será usada:\n{api_url}")
    print(f"Os dados serão salvos em: {os.path.abspath(output_file)}")
    # input("Pressione Enter para continuar...") # Pausa opcional

    coleta_dados_sidra(api_url, output_file)
    print(f"✅ Script de coleta da SIDRA ({os.path.basename(__file__)}) finalizado.")