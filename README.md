# 📊 View Enterprise - Pipeline de Processamento de Dados Empresariais

---

## 📄 Abstract

**View Enterprise** is a distributed data orchestration and processing system designed to extract, transform, and load business information from two public APIs. Automated by an Airflow DAG, the system queries data daily from DynamoDB, processes it using PySpark within an AWS Glue Job, stores the partitioned results in an S3 bucket, and delivers analytical visualizations through dashboards in Amazon QuickSight.

---

## 🌐 Visão Geral

O projeto foi desenvolvido com o objetivo de oferecer um fluxo automatizado de coleta e enriquecimento de dados de CNPJs, permitindo análises diárias com atualização contínua dos dados empresariais brasileiros.

### 🚀 Como funciona:
- Uma **DAG PySpark** executada via **Airflow** roda diariamente;
- Ela consulta o **DynamoDB** e verifica se existem CNPJs associados ao dia do processamento;
- Se houver, realiza o fetch dos dados a partir da **Receita Federal** e da **Brasil API**;
- Os dados são integrados, transformados e salvos no **S3 em formato Parquet particionado**;
- A visualização dos dados é feita através de **dashboards interativos no Amazon QuickSight**.

---

## 🧰 Stacks e Tecnologias Utilizadas

| Tecnologia     | Finalidade                                                |
|----------------|-----------------------------------------------------------|
| **PySpark**    | Processamento paralelo e transformações de dados          |
| **Airflow**    | Agendamento e orquestração de DAGs                        |
| **Glue Job**   | Execução de código PySpark na AWS                         |
| **DynamoDB**   | Armazenamento da lista de CNPJs por data de execução      |
| **Amazon S3**  | Armazenamento de dados processados (formato Parquet)      |
| **QuickSight** | Visualização de dados e geração de dashboards analíticos  |

---

## 📈 Visualizações no QuickSight

As análises disponíveis no painel incluem:

- Evolução do **capital social** das empresas ao longo do tempo;
- Lista das **empresas mais antigas**;
- **Distribuição por país**, destacando o país com mais empresas cadastradas.

---

## 📦 Estrutura Final dos Dados

Após o processamento, o resultado final é armazenado com a seguinte estrutura de dados:

```json
{
  "cnpj": "string",                        
  "pais": "string",                        
  "atividade_principal": { ... },         
  "nome": "string",                        
  "atividade_secundaria": { ... },        
  "telefone": "string",                   
  "data_inicio_atividade": "string",      
  "cnae_fiscal_descricao": "string",     
  "capital_social": number              
}
```

---

## 🗂️ Organização do Projeto

```bash
view-enterprises/
├── artifacts
├── README.md
└── src
    ├── devutils
    ├── etl
    ├── __init__.py
    ├── main.py
    ├── __pycache__
    ├── scoped_context.py
    ├── service
    └── tests
```

---

## 🛠️ Como Executar

1. Configure suas credenciais AWS (S3, Glue, DynamoDB e QuickSight).
2. Ajuste a DAG no Airflow (`dag_view_enterprise.py`) com os parâmetros do ambiente.
3. Rode o Airflow e agende a DAG para execução diária.
4. Os dados processados ficarão salvos no S3, disponíveis para análise via QuickSight.

---

## 🔮 Futuras Evoluções

- Validação automática de CNPJs antes do processamento;
- Versionamento dos datasets com Delta Lake;
- Enriquecimento com novas fontes de dados (como LinkedIn ou Receita estadual);
- Alertas por e-mail em caso de falha no pipeline.

---

## 📬 Contato do Desenvolvedor

Desenvolvido com dedicação por **Guilherme OliBr**  
📧 **Email:** guilherme.oliver12@gmail.com 
🌐 **GitHub:** [github.com/Guilherm12122](https://github.com/Guilherm12122)

---
