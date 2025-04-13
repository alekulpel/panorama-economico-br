# scripts/coleta_pib.py # Pode renomear para coleta_contas_nacionais.py se preferir

print("🚀 Iniciando script de coleta de dados das Contas Nacionais Trimestrais (SIDRA Tabela 6613)...") # Mensagem atualizada

import requests
import pandas as pd
import os
import logging

# Configuração do Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Parâmetros da Consulta ---
TABLE_ID = "6613"
GEO_LEVEL = "n1"
GEO_CODE = "1" # Usar '1' para Brasil é mais explícito que 'all' para n1
VARIABLES = "all" # Buscar todas as variáveis da tabela 6613
PERIODS = "all"
CLASSIFICATION = "c11255" # Setores Contas Nacionais Trimestrais
CATEGORIES = "all" # Pegar todos os setores
# Variável principal da tabela 6613: Índice de valor adicionado bruto a preços básicos com ajuste sazonal
# Formato 2 geralmente é o valor do índice
FORMAT_CODE = "v9319%202" # Display format para a variável principal

# Monta a URL da API de dados brutos - CORRIGIDO o typo 'hhttps'
api_url = f"https://apisidra.ibge.gov.br/values/t/{TABLE_ID}/{GEO_LEVEL}/{GEO_CODE}/v/{VARIABLES}/p/{PERIODS}/{CLASSIFICATION}/{CATEGORIES}/d/{FORMAT_CODE}"

# Diretório e nome do arquivo de saída - CORRIGIDO para caminho relativo
# Assume que o script está em 'scripts/' e os dados vão para 'data/raw/'
output_dir = "/home/akulpel/Projetos/panorama-economico-br/data/raw"
output_file = os.path.join(output_dir, "cnt_6613_setorial_dessazonalizado_bruto.csv") # Nome mais específico

def coleta_dados_sidra(url, filename):
    """
    Coleta dados da API SIDRA e salva em um arquivo CSV.
    """
    logging.info(f"Iniciando coleta da URL: {url}")
    try:
        # Usar verify=True é o ideal. Timeout aumentado para 90s.
        response = requests.get(url, verify=True, timeout=90)

        response.raise_for_status() # Verifica erros HTTP (4xx, 5xx)

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

        # --- Limpeza Básica (AJUSTADA para Tabela 6613 - Trimestral, Contas Nacionais) ---
        rename_map = {
            # Nomes comuns em tabelas trimestrais das Contas Nacionais (verifique a saída real!)
            'Valor': 'valor',
            'Trimestre (Código)': 'trimestre_codigo', # Ex: 202301 (Q1 2023), 202304 (Q4 2023)
            'Trimestre': 'trimestre_nome',          # Ex: '1º trimestre 2023'
            'Brasil (Código)': 'geo_cod',
            'Brasil': 'geo_nome',
            'Setores e subsetores (Código)': 'setor_cod', # Da classificação C11255
            'Setores e subsetores': 'setor_nome',
            'Variável (Código)': 'variavel_cod', # Se buscar v=all, terá essa coluna
            'Variável': 'variavel_nome'
            # Adicione/remova/ajuste conforme as colunas retornadas pela API para T=6613, V=all
        }
        # Renomeia apenas as colunas que existem no DataFrame
        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
        logging.info(f"Colunas após renomear: {df.columns.tolist()}")


        # Converter 'valor' para numérico, tratando possíveis não-números
        if 'valor' in df.columns:
            # Substitui '...' ou outros marcadores por NaN antes de converter
            df['valor'] = df['valor'].replace({'...': pd.NA, '-': pd.NA, 'X': pd.NA}, regex=False)
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
            logging.info("Coluna 'valor' convertida para numérico (erros viram NaN).")


        # Tratar período trimestral (Ex: '202301' -> Q1 2023)
        if 'trimestre_codigo' in df.columns:
            try:
                # Extrai ano e trimestre do formato YYYYQQ
                df['ano'] = df['trimestre_codigo'].str[:4].astype(int)
                df['trimestre_num'] = df['trimestre_codigo'].str[4:].astype(int)

                # Cria um objeto Period do Pandas (representa o trimestre)
                # Q1='01', Q2='02', Q3='03', Q4='04'
                df['periodo_trimestral'] = pd.PeriodIndex(year=df['ano'], quarter=df['trimestre_num'], freq='Q')

                # Opcional: Criar uma data de início ou fim do trimestre se preferir Datetime
                # df['data_inicio_trimestre'] = df['periodo_trimestral'].dt.start_time
                # df['data_fim_trimestre'] = df['periodo_trimestre'].dt.end_time

                logging.info("Coluna 'periodo_trimestral' (Pandas Period) criada a partir de 'trimestre_codigo'.")
                # Remove colunas auxiliares se não precisar mais delas
                # df.drop(columns=['ano', 'trimestre_num'], inplace=True)

            except Exception as e:
                logging.warning(f"Não foi possível converter 'trimestre_codigo' para período trimestral: {e}")


        # Garantir que o diretório de saída exista
        os.makedirs(output_dir, exist_ok=True) # Usando output_dir relativo

        # Salvar em CSV
        df.to_csv(output_file, index=False, encoding='utf-8') # Usando output_file com caminho relativo
        logging.info(f"✅ Dados brutos salvos em: {output_file}")

    except requests.exceptions.SSLError as e:
         logging.error(f"❌ Erro de SSL: {e}. Verifique a cadeia de certificados do seu sistema ou tente usar 'verify=False' (não recomendado para produção).")
         # print("\n--- TENTANDO NOVAMENTE COM verify=False ---")
         # # Código para tentar novamente com verify=False poderia ser adicionado aqui
         # try:
         #     response = requests.get(url, verify=False, timeout=90)
         #     # ... repetir lógica de processamento ...
         # except Exception as e_retry:
         #     logging.error(f"❌ Erro mesmo com verify=False: {e_retry}", exc_info=True)

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Erro de conexão com a API: {e}")
    except ValueError as e: # Erro ao decodificar JSON
        # Tenta pegar o response mesmo fora do try principal (pode falhar se o erro foi antes)
        response_text = "N/A"
        try:
            response_text = response.text[:500]
        except NameError:
             pass # response não foi definido
        logging.error(f"❌ Erro ao processar JSON da API: {e} - Conteúdo inicial: {response_text}")
    except Exception as e:
        logging.error(f"❌ Erro inesperado durante a coleta: {e}", exc_info=True) # Log completo do erro

# --- Execução ---
if __name__ == "__main__":
    # Teste a URL no navegador primeiro!
    print(f"URL da API que será usada:\n{api_url}")

    # Verifica se o diretório de saída existe antes de pausar (opcional)
    print(f"Os dados serão salvos em: {os.path.abspath(output_file)}") # Mostra caminho absoluto

    # input("Pressione Enter para continuar após verificar a URL e o caminho...") # Pausa opcional

    coleta_dados_sidra(api_url, output_file)
    print(f"✅ Script de coleta da SIDRA ({os.path.basename(__file__)}) finalizado.")