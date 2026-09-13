import os
import time
import textwrap
import boto3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker


# ============================================================================
# PARTE 1 - BRONZE (AWS Glue Job)
# ============================================================================
# ATENCAO: este bloco usa objetos do ambiente de execucao do Glue
# (glueContext, DynamicFrame, getResolvedOptions, etc.) que so existem
# quando o script roda como um Job do Glue. Ele NAO deve ser chamado a
# partir do main() deste arquivo - fica aqui apenas para manter todo o
# pipeline documentado em um unico lugar, em Python.
# ============================================================================

def job_bronze_glue():
    """
    Corpo do Glue Job que gera as 3 tabelas bronze a partir dos CSVs
    brutos no S3. Publique este bloco como um script de Job no AWS Glue
    (nao execute localmente).
    """
    import sys
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrameCollection
    from awsgluedq.transforms import EvaluateDataQuality
    from awsglue.dynamicframe import DynamicFrame

# ------------------------------------------------------------------
# CUSTOM TRANSFORM - Sanitizacao dos nomes de coluna
# Os CSVs de origem tem nomes de coluna "sujos" (com acentos,
# caracteres especiais, texto de pergunta completo, etc). Esta
# funcao limpa esses nomes para um padrao consistente (minusculo,
# sem acento, so letras/numeros/underscore), evitando problemas ao
# gravar no Glue Catalog e ao consultar depois via Athena.
# ------------------------------------------------------------------


def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    import re
    import unicodedata

    def sanitize_column_name(col_name):
        match = re.search(r",\s*'([^']*)'\s*\)$", col_name)
        if match:
            name = match.group(1)
        else:
            name = col_name
        name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
        name = name.lower()
        name = re.sub(r'[^a-z0-9]+', '_', name)
        name = re.sub(r'_+', '_', name).strip('_')
        return name if name else 'col_sem_nome'

    df = dfc.select(list(dfc.keys())[0]).toDF()
    novos_nomes = {}
    for old_col in df.columns:
        novo = sanitize_column_name(old_col)
        base = novo
        i = 1
        while novo in novos_nomes.values():
            novo = base + "_" + str(i)
            i += 1
        novos_nomes[old_col] = novo
    for old_col, new_col in novos_nomes.items():
        df = df.withColumnRenamed(old_col, new_col)
    dyf = DynamicFrame.fromDF(df, glueContext, "renamed")
    return DynamicFrameCollection({"renamed": dyf}, glueContext)


    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

    Arquivobronze_2023_2024 = glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False, "multiline": True},
        connection_type="s3",
        format="csv",
        connection_options={"paths": ["s3://fase-3-data/data_input/data_brasil_2023/"], "recurse": True},
        transformation_ctx="Arquivobronze_2023_2024",
    )

    Arquivobronze_2025_2026 = glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False, "multiline": True},
        connection_type="s3",
        format="csv",
        connection_options={"paths": ["s3://fase-3-data/data_input/data_brasil_2025/"], "recurse": True},
        transformation_ctx="Arquivobronze_2025_2026",
    )

    Arquivobronze_2024_2025 = glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False, "multiline": True},
        connection_type="s3",
        format="csv",
        connection_options={"paths": ["s3://fase-3-data/data_input/data_brasil_2024/"], "recurse": True},
        transformation_ctx="Arquivobronze_2024_2025",
    )

    CustomTransform_2023_2024 = MyTransform(
        glueContext, DynamicFrameCollection({"Arquivobronze_2023_2024": Arquivobronze_2023_2024}, glueContext),
    )
    CustomTransform_2025_2026 = MyTransform(
        glueContext, DynamicFrameCollection({"Arquivobronze_2025_2026": Arquivobronze_2025_2026}, glueContext),
    )
    CustomTransform_2024_2025 = MyTransform(
        glueContext, DynamicFrameCollection({"Arquivobronze_2024_2025": Arquivobronze_2024_2025}, glueContext),
    )

    EvaluateDataQuality().process_rows(
        frame=Arquivobronze_2023_2024,
        ruleset=DEFAULT_DATA_QUALITY_RULESET,
        publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1", "enableDataQualityResultsPublishing": True},
        additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"},
    )
    sink_2023_2024 = glueContext.getSink(
        path="s3://fase-3-data/data_output/data_brasil_2023/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        enableUpdateCatalog=True,
        transformation_ctx="sink_2023_2024",
    )
    sink_2023_2024.setCatalogInfo(catalogDatabase="default", catalogTableName="bronze_state_of_data_2023_2024")
    sink_2023_2024.setFormat("glueparquet", compression="snappy")
    sink_2023_2024.writeFrame(Arquivobronze_2023_2024)

    EvaluateDataQuality().process_rows(
        frame=Arquivobronze_2025_2026,
        ruleset=DEFAULT_DATA_QUALITY_RULESET,
        publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node2", "enableDataQualityResultsPublishing": True},
        additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"},
    )
    sink_2025_2026 = glueContext.getSink(
        path="s3://fase-3-data/data_output/data_brasil_2025/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        enableUpdateCatalog=True,
        transformation_ctx="sink_2025_2026",
    )
    sink_2025_2026.setCatalogInfo(catalogDatabase="default", catalogTableName="bronze_state_of_data_2025_2026")
    sink_2025_2026.setFormat("glueparquet", compression="snappy")
    sink_2025_2026.writeFrame(Arquivobronze_2025_2026)

    EvaluateDataQuality().process_rows(
        frame=Arquivobronze_2024_2025,
        ruleset=DEFAULT_DATA_QUALITY_RULESET,
        publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node3", "enableDataQualityResultsPublishing": True},
        additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"},
    )
    sink_2024_2025 = glueContext.getSink(
        path="s3://fase-3-data/data_output/data_brasil_2024/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        enableUpdateCatalog=True,
        transformation_ctx="sink_2024_2025",
    )
    sink_2024_2025.setCatalogInfo(catalogDatabase="default", catalogTableName="bronze_state_of_data_2024_2025")
    sink_2024_2025.setFormat("glueparquet", compression="snappy")
    sink_2024_2025.writeFrame(Arquivobronze_2024_2025)

    job.commit()


# ============================================================================
# CONFIGURACAO COMUM (Partes 2 e 3 - Athena)
# ============================================================================
ATHENA_DATABASE = "default"
ATHENA_OUTPUT_LOCATION = "s3://fase-3-data/athena_query_results/"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


def _parse_s3_uri(s3_uri):
    """Separa uma URL s3://bucket/prefixo/ em (bucket, prefixo)."""
    sem_esquema = s3_uri.replace("s3://", "")
    bucket, _, prefixo = sem_esquema.partition("/")
    return bucket, prefixo


def executar_query_athena(query, database=ATHENA_DATABASE, output_location=ATHENA_OUTPUT_LOCATION):
    """
    Executa uma query no Athena via boto3 e aguarda a conclusao.
    Retorna o QueryExecutionId, usado tanto para checar erros quanto
    para localizar o CSV de resultado que o Athena grava automaticamente
    no output_location.
    """
    client = boto3.client("athena")
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    query_execution_id = response["QueryExecutionId"]

    while True:
        status = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        motivo = status["QueryExecution"]["Status"].get("StateChangeReason", "motivo desconhecido")
        raise RuntimeError(f"Query Athena falhou ({state}): {motivo}")

    return query_execution_id


def executar_e_salvar_csv(query, nome_arquivo_csv, output_location=ATHENA_OUTPUT_LOCATION):
    """
    Executa uma query no Athena e baixa o resultado (que o Athena ja
    grava como CSV no output_location) para ./data/<nome_arquivo_csv>,
    pronto para ser lido pela Parte 4 (Graficos).
    """
    query_execution_id = executar_query_athena(query, output_location=output_location)

    bucket, prefixo = _parse_s3_uri(output_location)
    chave_s3 = f"{prefixo}{query_execution_id}.csv"

    os.makedirs(DATA_DIR, exist_ok=True)  # mesma protecao: garante a pasta mesmo se ela sumir entre a importacao e o download
    destino_local = os.path.join(DATA_DIR, nome_arquivo_csv)
    s3 = boto3.client("s3")
    s3.download_file(bucket, chave_s3, destino_local)
    print(f"  -> {nome_arquivo_csv} salvo em {destino_local}")


# ============================================================================
# PARTE 2 - SILVER
# Unifica as 3 edicoes da pesquisa em uma unica tabela, com nomes de
# coluna padronizados, ja que cada edicao nomeia as colunas de forma
# diferente no bronze.
# ============================================================================

QUERY_SILVER = """
CREATE TABLE silver_state_of_data_unificado
WITH (
    format = 'PARQUET',
    external_location = 's3://fase-3-data/data_output/silver/state_of_data_unificado/',
    write_compression = 'SNAPPY'
) AS

-- ===================== 2023-2024 =====================
SELECT
    NULLIF("_'p0'__'id'_#0", '') AS id_respondente,
    NULLIF(CAST("_'p1_a_'__'idade'_#1" AS VARCHAR), '') AS idade,
    NULLIF("_'p1_a_1_'__'faixa_idade'_#2", '') AS faixa_idade,
    NULLIF("_'p1_b_'__'genero'_#3", '')  AS genero,
    NULLIF("_'p1_c_'__'cor/raca/etnia'_#4", '') AS cor_raca_etnia,
    NULLIF("_'p1_d_'__'pcd'_#5", '') AS pcd,
    NULLIF("_'p1_i_1_'__'uf_onde_mora'_#23", '') AS uf_onde_mora,
    NULLIF("_'p1_i_2_'__'regiao_onde_mora'_#24", '') AS regiao_onde_mora,
    NULLIF("_'p1_l_'__'nivel_de_ensino'_#27", '') AS nivel_ensino,
    NULLIF("_'p1_m_'__'área_de_formação'_#28", '')AS area_formacao,
    NULLIF("_'p2_a_'__'qual_sua_situação_atual_de_trabalho?'_#29", '') AS situacao_trabalho,
    NULLIF("_'p2_b_'__'setor'_#30", '') AS setor,
    NULLIF("_'p2_c_'__'numero_de_funcionarios'_#31", '') AS numero_funcionarios,
    NULLIF("_'p2_f_'__'cargo_atual'_#34", '') AS cargo_atual,
    NULLIF("_'p2_g_'__'nivel'_#35", '') AS senioridade,
    NULLIF("_'p2_h_'__'faixa_salarial'_#36", '') AS faixa_salarial,
    CASE "_'p2_h_'__'faixa_salarial'_#36"
        WHEN 'Menos de R$ 1.000/mês' THEN 500
        WHEN 'de R$ 101/mês a R$ 2.000/mês' THEN 1500.5
        WHEN 'de R$ 1.001/mês a R$ 2.000/mês' THEN 1500.5
        WHEN 'de R$ 2.001/mês a R$ 3.000/mês' THEN 2500.5
        WHEN 'de R$ 3.001/mês a R$ 4.000/mês' THEN 3500.5
        WHEN 'de R$ 4.001/mês a R$ 6.000/mês' THEN 5000.5
        WHEN 'de R$ 6.001/mês a R$ 8.000/mês' THEN 7000.5
        WHEN 'de R$ 8.001/mês a R$ 12.000/mês' THEN 10000.5
        WHEN 'de R$ 12.001/mês a R$ 16.000/mês' THEN 14000.5
        WHEN 'de R$ 16.001/mês a R$ 20.000/mês' THEN 18000.5
        WHEN 'de R$ 20.001/mês a R$ 25.000/mês' THEN 22500.5
        WHEN 'de R$ 25.001/mês a R$ 30.000/mês' THEN 27500.5
        WHEN 'de R$ 25.001/mês a R$ 3000/mês' THEN 27500.5
        WHEN 'de R$ 30.001/mês a R$ 40.000/mês' THEN 35000.5
        WHEN 'Acima de R$ 40.001/mês' THEN 45000
        ELSE NULL
    END AS faixa_salarial_media,
    NULLIF("_'p2_r_'__'atualmente_qual_a_sua_forma_de_trabalho?'_#62", '') AS modelo_trabalho_atual,
    NULLIF("_'p2_s_'__'qual_a_forma_de_trabalho_ideal_para_você?'_#63", '') AS modelo_trabalho_ideal,
    NULLIF("_'p3_a_'__'qual_o_número_aproximado_de_pessoas_que_atuam_com_dados_na_sua_empresa_hoje?'_#65", '') AS numero_pessoas_dados_empresa,
    NULLIF("_'p3_e_'__'ai_generativa_é_uma_prioridade_em_sua_empresa?'_#103", '') AS ia_prioridade_empresa,
    NULLIF("_'p4_m_'__'utiliza_chatgpt_ou_llms_no_trabalho?'_#238", '') AS usa_chatgpt_llm,
    NULLIF("_'p4_i_'__'cloud_preferida'_#203", '') AS cloud_preferida,
    NULLIF("_'p4_k_'__'qual_sua_ferramenta_de_bi_preferida?'_#228", '') AS ferramenta_bi_preferida,
    NULLIF("_'p4_d_1_'__'sql'_#144", '') AS lang_sql,
    NULLIF("_'p4_d_2_'__'r_'_#145", '') AS lang_r,
    NULLIF("_'p4_d_3_'__'python'_#146", '') AS lang_python,
    NULLIF("_'p4_d_4_'__'c/c++/c#'_#147", '') AS lang_c_cpp_csharp,
    NULLIF("_'p4_d_7_'__'julia'_#150", '') AS lang_julia,
    NULLIF("_'p4_d_9_'__'visual_basic/vba'_#152", '') AS lang_vb_vba,
    NULLIF("_'p4_d_10_'__'scala'_#153", '') AS lang_scala,
    NULLIF("_'p4_d_12_'__'rust'_#155", '') AS lang_rust,
    CASE
        WHEN "_'p2_k_'__'você_está_satisfeito_na_sua_empresa_atual?'_#39" IN ('1', 'TRUE', 'true') THEN 'Sim'
        WHEN "_'p2_k_'__'você_está_satisfeito_na_sua_empresa_atual?'_#39" IN ('0', 'FALSE', 'false') THEN 'Nao'
        ELSE NULL
    END AS satisfeito_empresa,
    NULLIF("_'p2_l_'__'qual_o_principal_motivo_da_sua_insatisfação_com_a_empresa_atual?'_#40", '') AS motivo_insatisfacao,
    NULLIF("_'p2_n_'__'você_pretende_mudar_de_emprego_nos_próximos_6_meses?'_#49", '') AS planos_de_mudar_de_emprego_6m,
    '2023_2024' AS ano_pesquisa
FROM bronze_state_of_data_2023_2024

UNION ALL

-- ===================== 2024-2025 =====================
SELECT
    NULLIF("0.a_token", '') AS id_respondente,
    NULLIF(CAST("1.a_idade" AS VARCHAR), '')      AS idade,
    NULLIF("1.a.1_faixa_idade", '') AS faixa_idade,
    NULLIF("1.b_genero", '')AS genero,
    NULLIF("1.c_cor/raca/etnia", '') AS cor_raca_etnia,
    NULLIF("1.d_pcd", '')   AS pcd,
    NULLIF("1.i.1_uf_onde_mora", '') AS uf_onde_mora,
    NULLIF("1.i.2_regiao_onde_mora", '') AS regiao_onde_mora,
    NULLIF("1.l_nivel_de_ensino", '') AS nivel_ensino,
    NULLIF("1.m_área_de_formação", '') AS area_formacao,
    NULLIF("2.a_situação_de_trabalho", '') AS situacao_trabalho,
    NULLIF("2.b_setor", '') AS setor,
    NULLIF("2.c_numero_de_funcionarios", '') AS numero_funcionarios,
    NULLIF("2.f_cargo_atual", '')  AS cargo_atual,
    NULLIF("2.g_nivel", '') AS senioridade,
    NULLIF("2.h_faixa_salarial", '') AS faixa_salarial,
    CASE "2.h_faixa_salarial"
        WHEN 'Menos de R$ 1.000/mês' THEN 500
        WHEN 'de R$ 101/mês a R$ 2.000/mês' THEN 1500.5
        WHEN 'de R$ 1.001/mês a R$ 2.000/mês' THEN 1500.5
        WHEN 'de R$ 2.001/mês a R$ 3.000/mês' THEN 2500.5
        WHEN 'de R$ 3.001/mês a R$ 4.000/mês' THEN 3500.5
        WHEN 'de R$ 4.001/mês a R$ 6.000/mês' THEN 5000.5
        WHEN 'de R$ 6.001/mês a R$ 8.000/mês' THEN 7000.5
        WHEN 'de R$ 8.001/mês a R$ 12.000/mês' THEN 10000.5
        WHEN 'de R$ 12.001/mês a R$ 16.000/mês' THEN 14000.5
        WHEN 'de R$ 16.001/mês a R$ 20.000/mês' THEN 18000.5
        WHEN 'de R$ 20.001/mês a R$ 25.000/mês' THEN 22500.5
        WHEN 'de R$ 25.001/mês a R$ 30.000/mês' THEN 27500.5
        WHEN 'de R$ 25.001/mês a R$ 3000/mês' THEN 27500.5
        WHEN 'de R$ 30.001/mês a R$ 40.000/mês' THEN 35000.5
        WHEN 'Acima de R$ 40.001/mês' THEN 45000
        ELSE NULL
    END AS faixa_salarial_media,
    NULLIF("2.r_modelo_de_trabalho_atual", '') AS modelo_trabalho_atual,
    NULLIF("2.s_modelo_de_trabalho_ideal", '') AS modelo_trabalho_ideal,
    NULLIF("3.a_numero_de_pessoas_em_dados", '') AS numero_pessoas_dados_empresa,
    NULLIF("3.e_ai_generativa_e_llm_é_uma_prioridade?", '') AS ia_prioridade_empresa,
    NULLIF("4.m_usa_chatgpt_ou_copilot_no_trabalho?", '') AS usa_chatgpt_llm,
    NULLIF("4.i_cloud_preferida", '') AS cloud_preferida,
    NULLIF("4.k_ferramenta_de_bi_preferida", '') AS ferramenta_bi_preferida,
    NULLIF("4.d.1_sql", '') AS lang_sql,
    NULLIF("4.d.2_r", '') AS lang_r,
    NULLIF("4.d.3_python", '') AS lang_python,
    NULLIF("4.d.4_c/c++/c#", '') AS lang_c_cpp_csharp,
    NULLIF("4.d.7_julia", '') AS lang_julia,
    NULLIF("4.d.9_visual_basic/vba#90", '') AS lang_vb_vba,
    NULLIF("4.d.10_scala", '') AS lang_scala,
    NULLIF("4.d.12_rust", '') AS lang_rust,
    CASE
        WHEN "2.k_satisfeito_atualmente" IN ('1', 'TRUE', 'true') THEN 'Sim'
        WHEN "2.k_satisfeito_atualmente" IN ('0', 'FALSE', 'false') THEN 'Nao'
        ELSE NULL
    END AS satisfeito_empresa,

    NULLIF("2.l_motivo_insatisfacao", '') AS motivo_insatisfacao,
    NULLIF("2.n_planos_de_mudar_de_emprego_6m", '') AS planos_de_mudar_de_emprego_6m,
    '2024_2025' AS ano_pesquisa
FROM bronze_state_of_data_2024_2025

UNION ALL

-- ===================== 2025-2026 =====================
SELECT
    NULLIF("0.a_token", '') AS id_respondente,
    NULLIF(CAST("1.a_idade" AS VARCHAR), '') AS idade,
    NULLIF("1.a.1_faixa_idade", '') AS faixa_idade,
    NULLIF("1.b_genero", '') AS genero,
    NULLIF("1.c_cor/raca/etnia", '') AS cor_raca_etnia,
    NULLIF("1.d_pcd", '') AS pcd,
    NULLIF("1.i.1_uf_onde_mora", '') AS uf_onde_mora,
    NULLIF("1.i.2_regiao_onde_mora", '') AS regiao_onde_mora,
    NULLIF("1.l_nivel_de_ensino", '') AS nivel_ensino,
    NULLIF("1.m_área_de_formação", '') AS area_formacao,
    NULLIF("2.a_situação_de_trabalho", '') AS situacao_trabalho,
    NULLIF("2.b_setor", '') AS setor,
    NULLIF("2.c_numero_de_funcionarios", '') AS numero_funcionarios,
    NULLIF("2.f_cargo_atual", '') AS cargo_atual,
    NULLIF("2.g_nivel", '') AS senioridade,
    NULLIF("2.h_faixa_salarial", '') AS faixa_salarial,
    CASE "2.h_faixa_salarial"
        WHEN 'Menos de R$ 1.000/mês' THEN 500
        WHEN 'de R$ 101/mês a R$ 2.000/mês' THEN 1500.5
        WHEN 'de R$ 1.001/mês a R$ 2.000/mês' THEN 1500.5
        WHEN 'de R$ 2.001/mês a R$ 3.000/mês' THEN 2500.5
        WHEN 'de R$ 3.001/mês a R$ 4.000/mês' THEN 3500.5
        WHEN 'de R$ 4.001/mês a R$ 6.000/mês' THEN 5000.5
        WHEN 'de R$ 6.001/mês a R$ 8.000/mês' THEN 7000.5
        WHEN 'de R$ 8.001/mês a R$ 12.000/mês' THEN 10000.5
        WHEN 'de R$ 12.001/mês a R$ 16.000/mês' THEN 14000.5
        WHEN 'de R$ 16.001/mês a R$ 20.000/mês' THEN 18000.5
        WHEN 'de R$ 20.001/mês a R$ 25.000/mês' THEN 22500.5
        WHEN 'de R$ 25.001/mês a R$ 30.000/mês' THEN 27500.5
        WHEN 'de R$ 25.001/mês a R$ 3000/mês' THEN 27500.5
        WHEN 'de R$ 30.001/mês a R$ 40.000/mês' THEN 35000.5
        WHEN 'Acima de R$ 40.001/mês' THEN 45000
        ELSE NULL
    END AS faixa_salarial_media,
    NULLIF("2.q_modelo_de_trabalho_atual", '') AS modelo_trabalho_atual,
    NULLIF("2.r_modelo_de_trabalho_ideal", '') AS modelo_trabalho_ideal,
    NULLIF("3.a_numero_de_pessoas_em_dados", '') AS numero_pessoas_dados_empresa,
    NULLIF("3.e_ai_generativa_e_llm_é_uma_prioridade?", '') AS ia_prioridade_empresa,
    NULLIF("4.j_usa_chatgpt_ou_copilot_no_trabalho?", '')   AS usa_chatgpt_llm,
    NULLIF("4.f_cloud_preferida", '') AS cloud_preferida,
    NULLIF("4.h_ferramenta_de_bi_preferida", '')    AS ferramenta_bi_preferida,
    NULLIF("4.c.1_sql", '') AS lang_sql,
    NULLIF("4.c.2_r", '') AS lang_r,
    NULLIF("4.c.3_python", '') AS lang_python,
    NULLIF("4.c.4_c/c++/c#", '') AS lang_c_cpp_csharp,
    NULLIF("4.c.5_julia", '') AS lang_julia,
    NULLIF("4.c.6_visual_basic/vba#86", '') AS lang_vb_vba,
    NULLIF("4.c.7_scala", '') AS lang_scala,
    NULLIF("4.c.9_rust", '') AS lang_rust,
    CASE
        WHEN "2.k_satisfeito_atualmente" IN ('1', 'TRUE', 'true') THEN 'Sim'
        WHEN "2.k_satisfeito_atualmente" IN ('0', 'FALSE', 'false') THEN 'Nao'
        ELSE NULL
    END AS satisfeito_empresa,
    NULLIF("2.l_motivo_insatisfacao", '')         AS motivo_insatisfacao,
    NULLIF("2.n_planos_de_mudar_de_emprego_6m", '') AS planos_de_mudar_de_emprego_6m,
    '2025_2026' AS ano_pesquisa
FROM bronze_state_of_data_2025_2026
"""

def criar_silver():
    """Cria a tabela silver_state_of_data_unificado no Athena com overwrite."""
    print("Preparando o ambiente")
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('fase-3-data')
    bucket.objects.filter(Prefix='data_output/silver/state_of_data_unificado/').delete()

    executar_query_athena("DROP TABLE IF EXISTS silver_state_of_data_unificado")

    print("Criando tabela Silver")
    executar_query_athena(QUERY_SILVER)
    print("Tabela criada com sucesso.\n")


# ============================================================================
# PARTE 3 - GOLD
# As 3 perguntas de negocio. Cada sub-item (1a, 1b, ... 3d) e uma query
# independente, executada no Athena e exportada como CSV para ./data/,
# com o mesmo nome de arquivo esperado pela Parte 4 (Graficos).
# ============================================================================

# ---------------------- Pergunta 1: Estrutura do mercado ----------------------
QUERY_1A_CARGOS = """
-- 1a. Cargos mais comuns no mercado
SELECT 
    cargo_atual, 
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentual
FROM silver_state_of_data_unificado
WHERE 
    cargo_atual IS NOT NULL
GROUP BY cargo_atual
ORDER BY total DESC
LIMIT 15
"""

QUERY_1B_SENIORIDADE_ANO = """
-- 1b. Distribuicao por senioridade, por ano
SELECT 
    senioridade, 
    ano_pesquisa, 
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY ano_pesquisa), 1) AS percentual
FROM silver_state_of_data_unificado
WHERE 
    senioridade IS NOT NULL
GROUP BY 
    senioridade, 
    ano_pesquisa
ORDER BY 
    ano_pesquisa, 
    total DESC
"""

QUERY_1C_SETOR = """
-- 1c. Distribuicao por setor da empresa
SELECT 
    setor, 
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentual
FROM silver_state_of_data_unificado
WHERE setor IS NOT NULL
GROUP BY setor
ORDER BY total DESC
LIMIT 10
"""

QUERY_1D_MODELO_TRABALHO = """
-- 1d. Distribuicao por modelo de trabalho atual
SELECT 
    modelo_trabalho_atual, 
    ano_pesquisa, 
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY ano_pesquisa), 1) AS percentual
FROM silver_state_of_data_unificado
WHERE 
    modelo_trabalho_atual IS NOT NULL
GROUP BY 
    modelo_trabalho_atual, 
    ano_pesquisa
ORDER BY 
    ano_pesquisa, 
    total DESC
"""

# ------------------ Pergunta 2: Salario por cargo/senioridade ------------------
QUERY_2A_CARGO_SENIORIDADE = """
-- 2a. Top cargo + senioridade por salario medio
SELECT 
    cargo_atual, 
    senioridade,
    ROUND(AVG(faixa_salarial_media), 2) AS salario_medio,
    COUNT(*) AS total
FROM silver_state_of_data_unificado
WHERE 
    cargo_atual IS NOT NULL 
    AND senioridade IS NOT NULL 
    AND faixa_salarial_media IS NOT NULL
GROUP BY 
    cargo_atual, 
    senioridade
HAVING COUNT(*) >= 15
ORDER BY salario_medio DESC
LIMIT 15;
"""

QUERY_2B_SENIORIDADE_ANO = """
-- 2b. Evolucao do salario medio por senioridade, ao longo dos 3 anos
SELECT 
    senioridade, 
    ano_pesquisa,
    ROUND(AVG(faixa_salarial_media), 2) AS salario_medio,
    COUNT(*) AS total
FROM silver_state_of_data_unificado
WHERE 
    senioridade IS NOT NULL 
    AND faixa_salarial_media IS NOT NULL
GROUP BY 
    senioridade, 
    ano_pesquisa
ORDER BY 
    senioridade, 
    ano_pesquisa;
"""

QUERY_2C_NUMERO_FUNCIONARIOS = """
-- 2c. Salario medio por porte da empresa
SELECT 
    numero_funcionarios,
    ROUND(AVG(faixa_salarial_media), 2) AS salario_medio,
    COUNT(*) AS total
FROM silver_state_of_data_unificado
WHERE 
    numero_funcionarios IS NOT NULL 
    AND faixa_salarial_media IS NOT NULL
GROUP BY numero_funcionarios
HAVING COUNT(*) >= 15
ORDER BY salario_medio DESC;
"""

QUERY_2D_QTD_LINGUAGENS = """
-- 2d. Salario medio conforme quantidade de linguagens dominadas
SELECT
    (CAST(lang_sql AS INT) + CAST(lang_python AS INT) + CAST(lang_r AS INT) + CAST(lang_c_cpp_csharp AS INT) + CAST(lang_julia AS INT) + CAST(lang_vb_vba AS INT) + CAST(lang_scala AS INT) + CAST(lang_rust AS INT)) AS qtd_linguagens,
    ROUND(AVG(faixa_salarial_media), 2) AS salario_medio,
    COUNT(*) AS total
FROM silver_state_of_data_unificado
WHERE faixa_salarial_media IS NOT NULL
GROUP BY 1
HAVING COUNT(*) >= 15
ORDER BY qtd_linguagens
"""

# --------------------- Pergunta 3: Diversidade (genero/raca) ---------------------
QUERY_3A_GENERO_ANO = """
-- 3a. Representatividade de genero, por ano
SELECT 
    ano_pesquisa, 
    genero, 
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY ano_pesquisa), 1) AS percentual
FROM silver_state_of_data_unificado
WHERE genero IS NOT NULL
GROUP BY 
    ano_pesquisa, 
    genero
ORDER BY 
    ano_pesquisa, 
    percentual DESC;
"""

QUERY_3B_RACA_ANO = """
-- 3b. Representatividade de raca/etnia, por ano
SELECT 
    ano_pesquisa, 
    cor_raca_etnia, 
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY ano_pesquisa), 1) AS percentual
FROM silver_state_of_data_unificado
WHERE cor_raca_etnia IS NOT NULL
GROUP BY 
    ano_pesquisa, 
    cor_raca_etnia
ORDER BY 
    ano_pesquisa, 
    percentual DESC
"""

QUERY_3C_GAP_GENERO_SENIORIDADE = """
-- 3c. Gap salarial entre generos, controlado por senioridade
SELECT 
    senioridade, 
    genero,
    ROUND(AVG(faixa_salarial_media), 2) AS salario_medio,
    COUNT(*) AS total
FROM silver_state_of_data_unificado
WHERE 
    genero IN ('Masculino', 'Feminino') 
    AND senioridade IS NOT NULL 
    AND faixa_salarial_media IS NOT NULL
GROUP BY 
    senioridade, 
    genero
ORDER BY 
    senioridade, 
    genero
"""

QUERY_3D_GAP_RACA_SENIORIDADE = """
-- 3d. Gap salarial entre racas, controlado por senioridade
SELECT 
    senioridade, 
    cor_raca_etnia,
    ROUND(AVG(faixa_salarial_media), 2) AS salario_medio,
    COUNT(*) AS total
FROM silver_state_of_data_unificado
WHERE  
    cor_raca_etnia IS NOT NULL 
    AND senioridade IS NOT NULL 
    AND faixa_salarial_media IS NOT NULL
GROUP BY 
    senioridade, 
    cor_raca_etnia
HAVING COUNT(*) >= 15
ORDER BY 
    senioridade, 
    salario_medio DESC;
"""
# ---------------------- Pergunta 4: Tecnologias mais adotadas ----------------------
QUERY_4_TECNOLOGIAS = """
-- 4. Tecnologias (linguagens) mais adotadas pelos profissionais
SELECT 
    'SQL' AS tecnologia, 
    SUM(CAST(lang_sql AS INT)) AS total_usuarios 
FROM silver_state_of_data_unificado

UNION ALL

SELECT 
    'Python', 
    SUM(CAST(lang_python AS INT)) 
FROM silver_state_of_data_unificado

UNION ALL

SELECT 
    'R', 
    SUM(CAST(lang_r AS INT)) 
FROM silver_state_of_data_unificado

UNION ALL

SELECT 
    'C/C++/C#', 
    SUM(CAST(lang_c_cpp_csharp AS INT)) 
FROM silver_state_of_data_unificado

UNION ALL

SELECT 
    'Scala', 
    SUM(CAST(lang_scala AS INT)) 
FROM silver_state_of_data_unificado

UNION ALL

SELECT 
    'Julia', 
    SUM(CAST(lang_julia AS INT)) 
FROM silver_state_of_data_unificado

UNION ALL

SELECT 
    'VB/VBA', 
    SUM(CAST(lang_vb_vba AS INT)) 
FROM silver_state_of_data_unificado

UNION ALL

SELECT 
    'Rust', 
    SUM(CAST(lang_rust AS INT)) 
FROM silver_state_of_data_unificado

ORDER BY total_usuarios DESC
"""

# ---------------------- Pergunta 5: Adocao e impacto de IA ----------------------
QUERY_5_ADOCAO_IA = """
-- 5. Adocao de IA generativa nas empresas e impacto no salario medio
SELECT 
    ia_prioridade_empresa,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentual,
    ROUND(AVG(faixa_salarial_media), 2) AS salario_medio
FROM silver_state_of_data_unificado
WHERE ia_prioridade_empresa IS NOT NULL
GROUP BY ia_prioridade_empresa
ORDER BY total DESC
"""

# ---------------------- Pergunta 6: Diferencas regionais ----------------------
QUERY_6_REGIAO = """
-- 6. Salario medio por regiao do pais
SELECT 
    regiao_onde_mora,
    ROUND(AVG(faixa_salarial_media), 2) AS salario_medio,
    COUNT(*) AS total
FROM silver_state_of_data_unificado
WHERE regiao_onde_mora IS NOT NULL AND faixa_salarial_media IS NOT NULL
GROUP BY regiao_onde_mora
HAVING COUNT(*) >= 15
ORDER BY salario_medio DESC
"""

# Mapa: (query, nome do CSV de saida em ./data/) - usado pelo executor abaixo
CONSULTAS_GOLD = [
    ("Pergunta 1a - Cargos mais comuns", QUERY_1A_CARGOS, "q1_cargos.csv"),
    ("Pergunta 1b - Senioridade por ano", QUERY_1B_SENIORIDADE_ANO, "q1_senioridade_ano.csv"),
    ("Pergunta 1c - Distribuicao por setor", QUERY_1C_SETOR, "q1_setor.csv"),
    ("Pergunta 1d - Modelo de trabalho", QUERY_1D_MODELO_TRABALHO, "q1_modelo_trabalho.csv"),
    ("Pergunta 2a - Salario por cargo/senioridade", QUERY_2A_CARGO_SENIORIDADE, "q2_cargo_senioridade.csv"),
    ("Pergunta 2b - Salario por senioridade/ano", QUERY_2B_SENIORIDADE_ANO, "q2_senioridade_ano.csv"),
    ("Pergunta 2c - Salario por porte de empresa", QUERY_2C_NUMERO_FUNCIONARIOS, "q2_numero_funcionarios.csv"),
    ("Pergunta 2d - Salario por qtd. de linguagens", QUERY_2D_QTD_LINGUAGENS, "q2_qtd_linguagens.csv"),
    ("Pergunta 3a - Representatividade de genero", QUERY_3A_GENERO_ANO, "q3_genero_ano.csv"),
    ("Pergunta 3b - Representatividade de raca/etnia", QUERY_3B_RACA_ANO, "q3_raca_ano.csv"),
    ("Pergunta 3c - Gap salarial por genero", QUERY_3C_GAP_GENERO_SENIORIDADE, "q3_gap_genero_senioridade.csv"),
    ("Pergunta 3d - Gap salarial por raca/etnia", QUERY_3D_GAP_RACA_SENIORIDADE, "q3_gap_raca_senioridade.csv"),
    ("Pergunta 4 - Tecnologias mais adotadas", QUERY_4_TECNOLOGIAS, "q4_tecnologias.csv"),
    ("Pergunta 5 - Adocao e impacto de IA", QUERY_5_ADOCAO_IA, "q5_adocao_ia.csv"),
    ("Pergunta 6 - Diferencas regionais", QUERY_6_REGIAO, "q6_regiao.csv"),
]


def criar_gold():
    print("Gerando camada Gold")
    for descricao, query, nome_csv in CONSULTAS_GOLD:
        print(f"  {descricao}")
        executar_e_salvar_csv(query, nome_csv)
    print("Camada gerada com sucesso.\n")

# ============================================================================
# PARTE 4 - GRAFICOS EXECUTIVOS
# Le os CSVs gerados na Parte 3 (./data/) e monta 6 graficos executivos
# (2 por pergunta), no padrao visual AWS. Salva os PNGs em ./output/.
# ============================================================================

# ----------------------- Paleta de cores - padrao AWS -----------------------
COLOR_PRIMARY_VIVID = "#FF5722" # Laranja vivo / coral vibrante (destaques)
COLOR_BLUE_VIVID = "#007ACC" # Azul elétrico moderno
COLOR_NEUTRAL_BAR = "#90A4AE" # Cinza neutro elegante para barras secundárias
COLOR_TEXT_DARK = "#1E293B" # Texto escuro para máxima legibilidade
COLOR_MUTED = "#64748B" # Elementos secundários

COR_MASCULINO = "#007ACC" # Azul elétrico
COR_FEMININO = "#E91E63" # Rosa magenta vivo
CORES_VIVAS = ["#90A4AE", "#007ACC", "#FF5722", "#9C27B0", "#009688"]


def set_aws_style():
    plt.rcParams.update({
        "figure.facecolor": "#FFFFFF",
        "axes.facecolor": "#FFFFFF",
        "axes.edgecolor": "#CBD5E1",
        "axes.linewidth": 1.0,
        "axes.labelcolor": COLOR_TEXT_DARK,
        "axes.titlecolor": COLOR_TEXT_DARK,
        "text.color": COLOR_TEXT_DARK,
        "xtick.color": COLOR_TEXT_DARK,
        "ytick.color": COLOR_TEXT_DARK,
        "font.family": "sans-serif",
        "font.sans-serif": ["DejaVu Sans", "Arial", "Helvetica", "sans-serif"],
        "font.size": 11,
        "axes.titlesize": 14,
        "axes.titleweight": "bold",
        "axes.labelsize": 11,
        "axes.labelweight": "medium",
        "axes.grid": False,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "figure.dpi": 200,
    })


def formata_reais(x, _pos=None):
    return f"R$ {x:,.0f}".replace(",", ".")


def formata_milhar(x, _pos=None):
    return f"{x:,.0f}".replace(",", ".")


def salvar(fig, nome_arquivo):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    caminho = os.path.join(OUTPUT_DIR, nome_arquivo)
    fig.tight_layout()
    fig.savefig(caminho, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor(), edgecolor='none')
    plt.close(fig)
    print(f"✅ Gráfico salvo em: {caminho}")


# --------------------- Pergunta 1 - Gráfico 1: Cargos mais comuns ---------------------
def grafico_cargos_mais_comuns():
    df = pd.read_csv(os.path.join(DATA_DIR, "q1_cargos.csv"))
    mapa_normalizacao = {
        "Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect":
            "Engenheiro de Dados/Data Engineer/Data Architect",
    }
    df["cargo_atual"] = df["cargo_atual"].replace(mapa_normalizacao)
    agg = df.groupby("cargo_atual", as_index=False)["total"].sum()
    agg["percentual"] = (agg["total"] / agg["total"].sum() * 100).round(1)
    agg = agg.sort_values("total", ascending=True).tail(12)

    cores_barras = [COLOR_NEUTRAL_BAR] * (len(agg) - 1) + [COLOR_BLUE_VIVID]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(10, 6.5))
    bars = ax.barh(agg["cargo_atual"], agg["total"], color=cores_barras, height=0.65, edgecolor="none")
    
    max_val = agg["total"].max()
    for bar, pct in zip(bars, agg["percentual"]):
        ax.text(bar.get_width() + max_val * 0.015, bar.get_y() + bar.get_height() / 2,
                f"{pct}%", va="center", ha="left", fontsize=10, fontweight="bold", color=COLOR_TEXT_DARK)

    ax.set_title("Cargos Mais Comuns no Mercado de Dados", pad=15)
    ax.set_xlabel("Número de Respondentes")
    ax.set_xlim(0, max_val * 1.22)
    salvar(fig, "q1_cargos_mais_comuns.png")


# ---------------- Pergunta 1 - Gráfico 2: Distribuição de senioridade por edição ----------------
def grafico_senioridade_por_ano():
    caminho = os.path.join(DATA_DIR, "q1_senioridade_ano.csv")
    if not os.path.exists(caminho):
        return

    df = pd.read_csv(caminho)
    ordem_senioridade = ["Júnior", "Pleno", "Sênior", "Especialista/Staff+"]
    pivot = df.pivot(index="ano_pesquisa", columns="senioridade", values="percentual")
    cols_presentes = [c for c in ordem_senioridade if c in pivot.columns]
    pivot = pivot[cols_presentes].fillna(0)

    set_aws_style()
    fig, ax = plt.subplots(figsize=(9, 6))
    pivot.plot(kind="bar", ax=ax, color=CORES_VIVAS[:len(cols_presentes)], width=0.7, edgecolor="none")
    
    ax.set_title("Evolução da Distribuição de Senioridade por Edição", pad=15)
    ax.set_ylabel("Percentual dos Respondentes (%)")
    ax.set_xlabel("")
    plt.xticks(rotation=0)
    ax.legend(title="Senioridade", frameon=False, bbox_to_anchor=(0.5, -0.15), loc="upper center", ncol=4, fontsize=10, title_fontsize=11)
    
    max_val = pivot.values.max()
    if pd.isna(max_val) or max_val <= 0:
        max_val = 100.0
    ax.set_ylim(0, max_val * 1.25)
    
    salvar(fig, "q1_senioridade_por_ano.png")

# ---------------- Pergunta 1 - Gráfico 3: Distribuição por setor ----------------
def grafico_distribuicao_setor():
    df = pd.read_csv(os.path.join(DATA_DIR, "q1_setor.csv"))
    df = df.sort_values("total", ascending=True)

    cores_barras = [COLOR_NEUTRAL_BAR] * (len(df) - 1) + [COLOR_BLUE_VIVID]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(10, 6.5))
    bars = ax.barh(df["setor"], df["total"], color=cores_barras, height=0.65, edgecolor="none")

    max_val = df["total"].max()
    for bar, pct in zip(bars, df["percentual"]):
        ax.text(bar.get_width() + max_val * 0.015, bar.get_y() + bar.get_height() / 2,
                f"{pct}%", va="center", ha="left", fontsize=10, fontweight="bold", color=COLOR_TEXT_DARK)

    ax.set_title("Distribuição de Profissionais por Setor da Empresa", pad=15)
    ax.set_xlabel("Número de Respondentes")
    ax.set_xlim(0, max_val * 1.22)
    salvar(fig, "q1_distribuicao_setor.png")

# -------------- Pergunta 2 - Gráfico 1: Top cargo + senioridade por salário --------------
def grafico_top_perfis_valorizados():
    df = pd.read_csv(os.path.join(DATA_DIR, "q2_cargo_senioridade.csv"))

    df["cargo_atual"] = df["cargo_atual"].str.split("/").str[0].str.strip()

    df["soma_ponderada"] = df["salario_medio"] * df["total"]
    agg = df.groupby(["cargo_atual", "senioridade"], as_index=False).agg(
        soma_ponderada=("soma_ponderada", "sum"),
        total=("total", "sum"),
    )
    agg["salario_medio"] = agg["soma_ponderada"] / agg["total"]
    agg["label"] = agg["cargo_atual"] + " (" + agg["senioridade"] + ")"
    agg = agg.sort_values("salario_medio", ascending=True).tail(12)

    cores_barras = [COLOR_NEUTRAL_BAR] * (len(agg) - 1) + [COLOR_PRIMARY_VIVID]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(10.5, 7))
    bars = ax.barh(agg["label"], agg["salario_medio"], color=cores_barras, height=0.65, edgecolor="none")

    max_sal = agg["salario_medio"].max()
    for bar in bars:
        val = bar.get_width()
        ax.text(val + max_sal * 0.01, bar.get_y() + bar.get_height() / 2,
                formata_reais(val), va="center", ha="left", fontsize=9.5, fontweight="bold", color=COLOR_TEXT_DARK)

    ax.set_title("Perfis Profissionais Mais Valorizados (Salário Médio)", pad=15)
    ax.set_xlabel("Salário Médio (R$)")
    ax.xaxis.set_major_locator(mticker.MaxNLocator(nbins=6))
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(formata_reais))
    ax.set_xlim(0, max_sal * 1.28)
    salvar(fig, "q2_top_perfis_valorizados.png")


# ------------- Pergunta 2 - Gráfico 2: Impacto da qtd. de linguagens no salário -------------
def grafico_impacto_linguagens():
    df = pd.read_csv(os.path.join(DATA_DIR, "q2_qtd_linguagens.csv"))
    df = df.dropna(subset=["qtd_linguagens"])
    df["qtd_linguagens"] = df["qtd_linguagens"].astype(int)
    df = df.sort_values("qtd_linguagens")

    labels_x = [f"{n} lang." if n > 1 else f"{n} lang." for n in df["qtd_linguagens"]]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(9, 6))
    bars = ax.bar(labels_x, df["salario_medio"], color=COLOR_BLUE_VIVID, width=0.6, edgecolor="none")

    for bar, total in zip(bars, df["total"]):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 400,
            f"{formata_milhar(total)} pes.",
            ha="center", va="bottom", fontsize=9, fontweight="bold", color=COLOR_TEXT_DARK,
        )

    ax.set_title("Impacto do Domínio de Múltiplas Linguagens no Salário Médio", pad=25)
    ax.text(
        0.5, 1.03,
        "Base avaliada: SQL, Python, R, C/C++/C#, Julia, VB/VBA, Scala e Rust",
        transform=ax.transAxes, ha="center", fontsize=9.5, color=COLOR_MUTED, style="italic"
    )
    ax.set_xlabel("Quantidade de Linguagens Dominadas")
    ax.set_ylabel("Salário Médio (R$)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(formata_reais))
    ax.set_ylim(0, df["salario_medio"].max() * 1.25)
    salvar(fig, "q2_impacto_linguagens.png")

# ---------------- Pergunta 1/6 - Gráfico 4: Modelo de trabalho atual ----------------
def grafico_modelo_trabalho():
    df = pd.read_csv(os.path.join(DATA_DIR, "q1_modelo_trabalho.csv"))

    mapa_rotulos = {
        "Trabalho presencial": "100% presencial",
        "Modelo 100% presencial": "100% presencial",
        "Trabalho remoto": "100% remoto",
        "Modelo 100% remoto": "100% remoto",
    }
    df["modelo_curto"] = df["modelo_trabalho_atual"].map(mapa_rotulos).fillna(
        df["modelo_trabalho_atual"].str.slice(0, 30) + "..."
    )

    pivot = df.pivot_table(index="ano_pesquisa", columns="modelo_curto", values="percentual", aggfunc="sum").fillna(0)

    set_aws_style()
    fig, ax = plt.subplots(figsize=(10, 6.5))
    pivot.plot(kind="bar", ax=ax, color=CORES_VIVAS[:len(pivot.columns)], width=0.7, edgecolor="none")

    ax.set_title("Modelo de Trabalho Atual por Edição da Pesquisa", pad=15)
    ax.set_ylabel("Percentual dos Respondentes (%)")
    ax.set_xlabel("")
    ax.tick_params(axis='x', rotation=0)
    ax.legend(title="Modelo de trabalho", frameon=False, bbox_to_anchor=(0.5, -0.18), loc="upper center", ncol=2, fontsize=9, title_fontsize=10)

    max_val = pivot.values.max()
    ax.set_ylim(0, max_val * 1.25 if max_val > 0 else 100)
    salvar(fig, "q1_modelo_trabalho.png")
    
# -------------- Pergunta 3 - Gráfico 1: Representatividade de gênero por edição --------------
def grafico_representatividade_genero():
    df = pd.read_csv(os.path.join(DATA_DIR, "q3_genero_ano.csv"))
    pivot = df.pivot(index="ano_pesquisa", columns="genero", values="percentual")
    ordem = ["Masculino", "Feminino", "Outro", "Prefiro não informar"]
    ordem_presente = [c for c in ordem if c in pivot.columns]
    pivot = pivot[ordem_presente]
    cores = [COR_MASCULINO, COR_FEMININO, "#78909C", "#CFD8DC"]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(8.5, 6))
    pivot.plot(kind="bar", stacked=True, ax=ax, color=cores[:len(ordem_presente)], width=0.55, edgecolor="none")

    for i, ano in enumerate(pivot.index):
        acumulado = 0
        for genero in ordem_presente:
            valor = pivot.loc[ano, genero]
            if genero in ("Masculino", "Feminino") and valor > 5:
                ax.text(i, acumulado + valor / 2, f"{valor:.1f}%", ha="center", va="center",
                        fontsize=9.5, color="white", fontweight="bold")
            acumulado += valor

    ax.set_title("Evolução da Representatividade de Gênero nas Pesquisas", pad=15)
    ax.set_ylabel("Distribuição (%)")
    ax.set_xlabel("")
    ax.set_ylim(0, 105)
    plt.xticks(rotation=0)
    ax.legend(title="Gênero", frameon=False, bbox_to_anchor=(0.5, -0.15), loc="upper center", ncol=4, fontsize=10)
    salvar(fig, "q3_representatividade_genero.png")


# ----------------- Pergunta 3 - Gráfico 2: Gap salarial por gênero e senioridade -----------------
def grafico_gap_salarial_genero():
    df = pd.read_csv(os.path.join(DATA_DIR, "q3_gap_genero_senioridade.csv"))
    pivot = df.pivot(index="senioridade", columns="genero", values="salario_medio")
    ordem_senioridade = ["Júnior", "Pleno", "Sênior", "Especialista/Staff+"]
    pivot = pivot.reindex([s for s in ordem_senioridade if s in pivot.index])

    set_aws_style()
    fig, ax = plt.subplots(figsize=(9, 6))
    x = range(len(pivot.index))
    width = 0.35

    ax.bar([i - width / 2 for i in x], pivot["Masculino"], width, label="Masculino", color=COR_MASCULINO, edgecolor="none")
    ax.bar([i + width / 2 for i in x], pivot["Feminino"], width, label="Feminino", color=COR_FEMININO, edgecolor="none")

    for i, senioridade in enumerate(pivot.index):
        masc = pivot.loc[senioridade, "Masculino"]
        fem = pivot.loc[senioridade, "Feminino"]
        gap_pct = (masc - fem) / masc * 100
        y_max = max(masc, fem)
        ax.text(i, y_max + y_max * 0.02, f"Gap: {gap_pct:.1f}%",
                 ha="center", va="bottom", fontsize=9, color=COLOR_TEXT_DARK, fontweight="bold")

    ax.set_xticks(list(x))
    ax.set_xticklabels(pivot.index)
    ax.set_title("Gap Salarial por Gênero (Controlado por Senioridade)", pad=15)
    ax.set_ylabel("Salário Médio (R$)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(formata_reais))
    ax.legend(frameon=False, fontsize=10, loc="upper right")
    ax.set_ylim(0, pivot.values.max() * 1.25)
    salvar(fig, "q3_gap_salarial_genero.png")


# --------------------- Pergunta 4 - Gráfico: Tecnologias mais adotadas ---------------------
def grafico_tecnologias_mais_adotadas():
    df = pd.read_csv(os.path.join(DATA_DIR, "q4_tecnologias.csv"))
    df = df.sort_values("total_usuarios", ascending=True)

    cores_barras = [COLOR_NEUTRAL_BAR] * (len(df) - 1) + [COLOR_BLUE_VIVID]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(10, 6.5))
    bars = ax.barh(df["tecnologia"], df["total_usuarios"], color=cores_barras, height=0.65, edgecolor="none")
    
    max_val = df["total_usuarios"].max()
    for bar in bars:
        val = bar.get_width()
        ax.text(val + max_val * 0.01, bar.get_y() + bar.get_height() / 2,
                 formata_milhar(val), va="center", ha="left", fontsize=9.5, fontweight="bold", color=COLOR_TEXT_DARK)

    ax.set_title("Tecnologias e Ferramentas Mais Adotadas no Mercado", pad=15)
    ax.set_xlabel("Número de Profissionais Usuários")
    ax.set_xlim(0, max_val * 1.22)
    salvar(fig, "q4_tecnologias_mais_adotadas.png")


# --------------------- Pergunta 5 - Gráfico: Adoção e impacto de IA ---------------------
def grafico_adocao_ia():
    caminho = os.path.join(DATA_DIR, "q5_adocao_ia.csv")
    if not os.path.exists(caminho):
        return

    df = pd.read_csv(caminho)
    
    mapa_rotulos_curtos = {
        "Sim, está entre nossas principais prioridades para os próximos 2-4 anos (com discussões de iniciativas e orçamentos de curto a médio prazo).": 
            "Prioridade alta (2-4 anos)",
        "Sim, é nossa principal prioridade como empresa (com foco executivo significativo e alocação de orçamento relevante).": 
            "Principal prioridade estratégica",
        "Não é uma iniciativa que estamos focando e não tem sido uma prioridade.": 
            "Não é prioridade / Sem foco",
        "Mais ou menos... É uma das várias iniciativas que estamos impulsionando, mas não é uma prioridade (tratam-se de iniciativas isoladas e com pouco foco).": 
            "Iniciativa isolada (Baixa prioridade)",
        "Mais ou menos... É uma das várias iniciativas que estamos impulsionando, mas não é uma prioridade (iniciativas isoladas e pouco foco).": 
            "Iniciativa isolada (Baixa prioridade)",
        "Não sei opinar sobre esse assunto.": 
            "Não sabe / Não opinou"
    }

    if "ia_prioridade_empresa" in df.columns:
        df["rotulo_curto"] = df["ia_prioridade_empresa"].map(mapa_rotulos_curtos).fillna(df["ia_prioridade_empresa"])
    else:
        df["rotulo_curto"] = "Categoria"

    if "total" in df.columns and "salario_medio" in df.columns:
        df["soma_ponderada"] = df["salario_medio"] * df["percentual"]
        agg = df.groupby("rotulo_curto", as_index=False).agg(
            percentual=("percentual", "sum"),
            soma_ponderada=("soma_ponderada", "sum")
        )
        agg["salario_medio"] = agg["soma_ponderada"] / agg["percentual"]
    else:
        agg = df.groupby("rotulo_curto", as_index=False).agg(
            percentual=("percentual", "sum"),
            salario_medio=("salario_medio", "mean")
        )

    agg = agg.sort_values("percentual", ascending=True)

    cores_barras = [COLOR_NEUTRAL_BAR] * (len(agg) - 1) + [COLOR_PRIMARY_VIVID]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(agg["rotulo_curto"], agg["percentual"], color=cores_barras, height=0.6, edgecolor="none")

    max_pct = agg["percentual"].max()
    if pd.isna(max_pct) or max_pct <= 0:
        max_pct = 30.0

    for bar, salario, pct in zip(bars, agg["salario_medio"], agg["percentual"]):
        salario_str = formata_reais(salario) if not pd.isna(salario) else "R$ 0"
        texto = f"{pct:.1f}%  |  Média: {salario_str}"
        ax.text(bar.get_width() + max_pct * 0.015, bar.get_y() + bar.get_height() / 2,
                 texto, va="center", ha="left", fontsize=10, color=COLOR_TEXT_DARK, fontweight="bold")

    ax.set_title("Prioridade de IA Generativa nas Empresas x Salário Médio", pad=15)
    ax.set_xlabel("Adoção / Prioridade (% dos Respondentes)")
    ax.set_xlim(0, max_pct * 1.62)
    
    salvar(fig, "q5_adocao_impacto_ia.png")


# --------------------- Pergunta 6 - Gráfico: Diferenças regionais ---------------------
def grafico_diferencas_regionais():
    df = pd.read_csv(os.path.join(DATA_DIR, "q6_regiao.csv"))
    df = df.sort_values("salario_medio", ascending=True)

    cores_barras = [COLOR_NEUTRAL_BAR] * (len(df) - 1) + [COLOR_PRIMARY_VIVID]

    set_aws_style()
    fig, ax = plt.subplots(figsize=(9, 6))
    bars = ax.barh(df["regiao_onde_mora"], df["salario_medio"], color=cores_barras, height=0.6, edgecolor="none")
    
    max_sal = df["salario_medio"].max()
    for bar in bars:
        val = bar.get_width()
        ax.text(val + max_sal * 0.01, bar.get_y() + bar.get_height() / 2,
                formata_reais(val), va="center", ha="left", fontsize=9.5, fontweight="bold", color=COLOR_TEXT_DARK)

    ax.set_title("Panorama de Salário Médio por Região do País", pad=15)
    ax.set_xlabel("Salário Médio (R$)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(formata_reais))
    ax.set_xlim(0, max_sal * 1.25)
    salvar(fig, "q6_diferencas_regionais.png")

def gerar_graficos():
    print(" Iniciando geração dos gráficos com paleta viva e limpa...")
    
    grafico_cargos_mais_comuns()
    grafico_senioridade_por_ano()
    grafico_distribuicao_setor()      
    grafico_modelo_trabalho()         
    grafico_top_perfis_valorizados()
    grafico_impacto_linguagens()
    grafico_representatividade_genero()
    grafico_gap_salarial_genero()
    grafico_tecnologias_mais_adotadas()
    grafico_adocao_ia()
    grafico_diferencas_regionais()

    print(f"\n Todos os gráficos vivos e limpos foram salvos em: {OUTPUT_DIR}/")

if __name__ == "__main__":
    gerar_graficos()

# ============================================================================
# ORQUESTRACAO (Partes 2, 3 e 4)
# A Parte 1 (Bronze) NAO entra aqui - ela e publicada e executada
# separadamente como um Job do AWS Glue.
# ============================================================================

def main():
    criar_silver()
    criar_gold()
    gerar_graficos()


if __name__ == "__main__":
    main()