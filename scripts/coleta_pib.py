print("🚀 Iniciando script de coleta de dados do PIB...")

import requests
import pandas as pd
import os
import urllib3

# Desabilitar warnings SSL globalmente
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# URL ajustada para consulta ao PIB setorial
url = "https://apisidra.ibge.gov.br/values/t/6340/n1/BR/n2/1/p/all"

def coleta_dados_pib():
    try:
        response = requests.get(url, verify=False)  # Ignorando SSL
        response.raise_for_status()

        print("✅ Conexão bem-sucedida!")
        print("STATUS:", response.status_code)

        dados = response.json()
        print("🔍 Exemplo de dado retornado:")
        print(dados[0])

        # Primeiro item é o cabeçalho (nomes das colunas)
        colunas = list(dados[0].values())
        dados_limpos = [list(item.values()) for item in dados[1:]]

        df = pd.DataFrame(dados_limpos, columns=colunas)

        os.makedirs("data/raw", exist_ok=True)
        df.to_csv("data/raw/pib_setorial.csv", index=False)
        print("✅ Dados salvos em: data/raw/pib_setorial.csv")

    except Exception as e:
        print(f"❌ Erro: {e}")

coleta_dados_pib()


