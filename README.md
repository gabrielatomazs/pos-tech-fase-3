# Tech Challenge — Fase 3 | State of Data Brasil

Pipeline de Engenharia de Dados e Analytics construído sobre as 3 últimas edições da pesquisa **State of Data Brasil** (Data Hackers + Bain), simulando uma arquitetura Big Data em ambiente AWS para apoiar a expansão da área de Dados, Analytics e IA de uma Instituição Financeira fictícia.

---

## 🎯 Objetivo de negócio

Responder a 7 perguntas estratégicas sobre o mercado brasileiro de Dados e IA:

1. Como está estruturado o mercado brasileiro de Dados?
2. Quais perfis profissionais são mais valorizados pelo mercado?
3. Qual é o cenário de diversidade de gênero nas carreiras de dados?
4. Quais tecnologias apresentam maior adoção entre os profissionais?
5. Qual é o índice de adoção de Inteligência Artificial e seu impacto?
6. Existem diferenças relevantes entre regiões, senioridades ou modelos de trabalho?
7. Quais oportunidades e desafios podem ser identificados para empresas que desejam investir em Dados e IA?


## ☁️ Stack AWS utilizada

- **Amazon S3** — Data Lake (camadas Bronze, Silver e Gold)
- **AWS Glue** — ingestão, limpeza e catalogação (Data Catalog)
- **Amazon Athena** — consultas SQL sobre Silver e Gold
- **AWS IAM** (LabRole) — permissões de execução no AWS Academy Lab
- **Amazon CloudWatch** — logs de execução do Glue Job


## 👥 Autoria

Projeto desenvolvido por Gabriela V Tomaz Silva da Cruz e Giulia Ribeiro Santoro para Tech Challenge da Fase 3 — Pós Tech FIAP.

## 📄 Fonte dos dados

[State of Data Brasil — Data Hackers + Bain (Kaggle)](https://www.kaggle.com/datahackers/datasets)
