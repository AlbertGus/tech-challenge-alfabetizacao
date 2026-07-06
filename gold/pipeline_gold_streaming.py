import pandas as pd
import boto3
import io
import json
import time
from datetime import datetime

# ==============================================================================
# CONFIGURAÇÃO DE SEGURANÇA AWS
# ==============================================================================
AWS_ACCESS_KEY = '#####'
AWS_SECRET_KEY = '#####'
REGIAO_AWS = 'us-east-1'

BUCKET_SILVER = 'c-silver'
BUCKET_GOLD = 'c-gold'

print("1. A ligar à AWS S3 (Pipeline Gold para Streaming)...")
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGIAO_AWS
)


# ==============================================================================
# MÓDULO DE OBSERVABILIDADE E MONITORAMENTO (REQUISITO OPCIONAL DO TECH CHALLENGE)
# ==============================================================================
def registrar_log_monitoramento(status, latencia_segundos, volume_linhas, mensagem_erro=""):
    """
    Gera um log de execução (telemetria) e salva no Data Lake.
    Cobre: Falhas, Latência, Volume e Alertas.
    """
    timestamp_agora = datetime.utcnow().isoformat()
    log_data = {
        "pipeline": "silver_to_gold_streaming",
        "data_execucao": timestamp_agora,
        "status": status,
        "latencia_segundos": latencia_segundos,
        "volume_registros_processados": volume_linhas,
        "alerta_erro": mensagem_erro
    }

    # Salva o log no formato JSON para fácil leitura futura por ferramentas de monitoramento
    nome_arquivo_log = f"monitoramento/logs_gold/exec_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

    try:
        s3_client.put_object(
            Bucket=BUCKET_GOLD,
            Key=nome_arquivo_log,
            Body=json.dumps(log_data, indent=2),
            ContentType='application/json'
        )
        print(f"\n📊 [MONITORAMENTO] Log de observabilidade salvo com sucesso em: {nome_arquivo_log}")
    except Exception as e:
        print(f"\n⚠️ [ALERTA DE MONITORAMENTO] Falha ao salvar o log: {e}")


def processar_streaming_para_gold():
    # Variáveis de rastreamento para o Monitoramento
    inicio_pipeline = time.time()
    volume_processado = 0
    status_final = "SUCESSO"
    detalhe_erro = "Nenhum"

    try:
        # ======================================================================
        # 1. EXTRAÇÃO: Ler TODOS os micro-batches Parquet da Silver
        # ======================================================================
        print("\n2. A extrair os dados consolidados da Camada Silver...")

        resposta = s3_client.list_objects_v2(Bucket=BUCKET_SILVER, Prefix='streaming_processado/')

        if 'Contents' not in resposta:
            print("Nenhum dado processado encontrado na Silver.")
            status_final = "IGNORADO_SEM_DADOS"
            return

        lista_ficheiros = [obj['Key'] for obj in resposta['Contents'] if obj['Key'].endswith('.parquet')]

        dfs = []
        for ficheiro in lista_ficheiros:
            obj = s3_client.get_object(Bucket=BUCKET_SILVER, Key=ficheiro)
            df_temp = pd.read_parquet(io.BytesIO(obj['Body'].read()))
            dfs.append(df_temp)

        df_silver_streaming = pd.concat(dfs, ignore_index=True)
        volume_processado = len(df_silver_streaming)  # Captura o volume para o log!
        print(
            f"-> Foram lidos {len(lista_ficheiros)} ficheiro(s) Parquet. Total de medições de alunos: {volume_processado}")

        # ======================================================================
        # 2. TRANSFORMAÇÃO (LÓGICA DE NEGÓCIO): Agregação para Dashboards
        # ======================================================================
        print("\n3. A transformar e agregar dados para a Camada Gold (Visão Executiva)...")

        df_gold_streaming = df_silver_streaming.groupby('id_municipio').agg(
            total_alunos_avaliados=('id_aluno', 'count'),
            media_nota_tempo_real=('nota_avaliacao', 'mean'),
            ultima_atualizacao=('processado_em_silver', 'max')
        ).reset_index()

        df_gold_streaming['media_nota_tempo_real'] = df_gold_streaming['media_nota_tempo_real'].round(2)

        print("\n--- Tabela Gold (Desempenho em Tempo Real por Município) ---")
        display(df_gold_streaming.head())

        # ======================================================================
        # 3. CARGA: Guardar tabela analítica final
        # ======================================================================
        print("\n4. A carregar KPI's para a Camada Gold...")

        buffer_parquet = io.BytesIO()
        df_gold_streaming.to_parquet(buffer_parquet, index=False, engine='pyarrow')

        nome_ficheiro_gold = 'streaming_analitico/kpis_tempo_real.parquet'

        s3_client.put_object(
            Bucket=BUCKET_GOLD,
            Key=nome_ficheiro_gold,
            Body=buffer_parquet.getvalue()
        )

        print(f"\n🏆 SUCESSO ABSOLUTO! Tabela de KPIs em tempo real pronta para o Dashboard!")
        print(f"Caminho: s3://{BUCKET_GOLD}/{nome_ficheiro_gold}")

    except Exception as e:
        # Se falhar, capturamos o erro exato para o log
        status_final = "FALHA_CRITICA"
        detalhe_erro = str(e)
        print(f"\n❌ ERRO NO PIPELINE GOLD: {detalhe_erro}")

    finally:
        # Independentemente de dar sucesso ou erro, o bloco 'finally' sempre executa!
        # Isso garante que o Monitoramento sempre grave a latência e o status final.
        fim_pipeline = time.time()
        latencia_total = round(fim_pipeline - inicio_pipeline, 2)

        # Chama a função de observabilidade criada acima
        registrar_log_monitoramento(status_final, latencia_total, volume_processado, detalhe_erro)


# Executar a função
processar_streaming_para_gold()