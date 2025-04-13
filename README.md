# Insight Econômico BR: Análise e Previsão Setorial

📊 Projeto de ciência de dados para analisar e prever o Produto Interno Bruto (PIB) do Brasil com foco em sua decomposição setorial (agropecuária, indústria e serviços), utilizando dados reais do IBGE via API SIDRA.

## 🔍 Objetivos
- Coletar dados trimestrais do PIB por setor via API pública
- Analisar a evolução histórica e correlações econômicas
- Construir modelos de previsão para o PIB total e setorial
- Desenvolver um dashboard interativo com Streamlit

## 🧰 Tecnologias utilizadas
- Python (pandas, requests, numpy, matplotlib, seaborn, plotly)
- Machine Learning (regressão, modelagem temporal)
- Streamlit (dashboard interativo)
- Git e GitHub (controle de versão)
- API SIDRA do IBGE

## 🚀 Como executar o projeto

Clone o repositório e instale as dependências:

```bash
git clone https://github.com/SEU_USUARIO/panorama-economico-br.git
cd panorama-economico-br
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

#Para rodar notebooks ou dashboards:

jupyter notebook
# ou
streamlit run app/main.py

##📁 Estrutura do projeto

├── app/              # Código do dashboard Streamlit
├── data/             # Dados coletados da API
├── notebooks/        # Análises exploratórias e modelagem
├── scripts/          # Scripts utilitários de coleta/processamento
├── README.md
├── requirements.txt

##✍️ Autor
Alexandre Kulpel — linkedin.com/in/alekulpel



