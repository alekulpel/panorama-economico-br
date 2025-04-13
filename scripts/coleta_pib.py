print("🚀 Iniciando script de coleta de dados do PIB...")

import requests
import pandas as pd
import os

url = "https://api.sidra.ibge.gov.br/values/t/6340/n1/BR/n2/1/p/last"

def coleta_dados_pib():
    try:
        response = requests.get(url)
        response.raise_for_status()

        print("STATUS:", response.status_code)
        print("HEADERS:", response.headers.get('Content-Type'))

        dados = response.json()
        print("🔍 Primeiro item retornado:")
        print(dados[0])

        df = pd.DataFrame(dados)

        if df.empty:
            print("⚠️ DataFrame vazio. Verifique a estrutura dos dados.")
            return

        os.makedirs('data/raw', exist_ok=True)
        df.to_csv('data/raw/pib_setorial.csv', index=False)
        print("✅ Dados salvos em data/raw/pib_setorial.csv")

    except Exception as e:
        print(f"❌ Erro: {e}")

coleta_dados_pib()
