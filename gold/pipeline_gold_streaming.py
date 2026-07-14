import pandas as pd
import boto3
import io
import json
import time
from datetime import datetime, timezone

# ==============================================================================
# CONFIGURAÇÃO DE SEGURANÇA AWS
# ==============================================================================
AWS_ACCESS_KEY = '#####'
AWS_SECRET_KEY = '######'
REGIAO_AWS = 'us-east-1' 

BUCKET_SILVER = 'c-silver'
BUCKET_GOLD = 'c-gold'

s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=REGIAO_AWS)

# ==============================================================================
# MÓDULO DE OBSERVABILIDADE
# ==============================================================================
def registrar_log_monitoramento(status, latencia_segundos, volume_linhas, arquivo_gatilho, mensagem_erro=""):
    """Salva a telemetria da execução baseada em eventos."""
    log_data = {
        "pipeline": "silver_to_gold_streaming_event_driven",
        "data_execucao": datetime.now(timezone.utc).isoformat(),
        "gatilho_s3": arquivo_gatilho,
        "status": status,
        "latencia_segundos": latencia_segundos,
        "linhas_processadas": volume_linhas,
        "erro": mensagem_erro
    }
    nome_arquivo_log = f"monitoramento/logs_gold/exec_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    try:
        s3_client.put_object(Bucket=BUCKET_GOLD, Key=nome_arquivo_log, Body=json.dumps(log_data, indent=2))
        print(f"📊 [OBSERVABILIDADE] Log salvo em: {nome_arquivo_log}")
    except Exception as e:
        pass

# ==============================================================================
# ARQUITETURA ORIENTADA A EVENTOS (LAMBDA HANDLER)
# ==============================================================================
# Esta é a estrutura exata que a AWS exige. A nuvem dispara esta função 
# automaticamente assim que um arquivo novo cai na Camada Silver.
def lambda_handler(event, context):
    print("⚡ [EVENTO DETECTADO] Inicializando atualização da Gold em Tempo Real...")
    inicio_pipeline = time.time()
    status_final = "SUCESSO"
    detalhe_erro = "Nenhum"
    volume_processado = 0
    arquivo_gatilho = "Desconhecido"

    try:
        # 1. O evento S3 informa qual arquivo exato acabou de chegar na Silver
        registro = event['Records'][0]['s3']
        bucket_origem = registro['bucket']['name']
        arquivo_gatilho = registro['object']['key']
        
        print(f"-> Arquivo recebido pelo EventBridge/S3 Trigger: s3://{bucket_origem}/{arquivo_gatilho}")

        # 2. Lemos apenas este novo micro-batch (arquivo) para processamento rápido
        obj = s3_client.get_object(Bucket=bucket_origem, Key=arquivo_gatilho)
        df_novo_silver = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        volume_processado = len(df_novo_silver)

        print("\n3. Calculando KPIs agregados para atualizar o Dashboard...")
        
        # Agregação da nova carga
        df_atualizacao = df_novo_silver.groupby('id_municipio').agg(
            total_alunos_avaliados=('id_aluno', 'count'),
            media_nota_tempo_real=('nota_avaliacao', 'mean'),
            ultima_atualizacao=('processado_em_silver', 'max')
        ).reset_index()

        df_atualizacao['media_nota_tempo_real'] = df_atualizacao['media_nota_tempo_real'].round(2)

        # 4. SALVANDO NA GOLD
        # Salvando o micro-batch analítico para visualização.
        buffer_parquet = io.BytesIO()
        df_atualizacao.to_parquet(buffer_parquet, index=False, engine='pyarrow')

        nome_ficheiro_gold = f"streaming_analitico/kpis_batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
        
        s3_client.put_object(Bucket=BUCKET_GOLD, Key=nome_ficheiro_gold, Body=buffer_parquet.getvalue())

        print(f"\n🏆 SUCESSO! KPIs atualizados e salvos na Gold: s3://{BUCKET_GOLD}/{nome_ficheiro_gold}")
        
    except Exception as e:
        status_final = "FALHA_CRITICA"
        detalhe_erro = str(e)
        print(f"\n❌ ERRO NO PIPELINE: {detalhe_erro}")
        
    finally:
        latencia_total = round(time.time() - inicio_pipeline, 2)
        registrar_log_monitoramento(status_final, latencia_total, volume_processado, arquivo_gatilho, detalhe_erro)
        return {"statusCode": 200, "body": json.dumps("Processamento concluído.")}

# ==============================================================================
# SIMULAÇÃO DO GATILHO DA NUVEM (MOCK EVENT)
# ==============================================================================
# Simulamos o evento que o 
# AWS S3 enviaria para o nosso script. Buscando o arquivo
# mais recente na camada Silver
if __name__ == "__main__":
    print("🔍 Procurando um arquivo real na Silver para simular o gatilho da AWS...")
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_SILVER, Prefix="streaming_processado/")
        if 'Contents' in response and len(response['Contents']) > 0:
            # Ordena os arquivos pela data de modificação e pega o mais novo
            arquivos = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
            arquivo_real_mais_recente = arquivos[0]['Key']
            
            evento_s3_mock = {
              "Records": [
                {
                  "s3": {
                    "bucket": {"name": BUCKET_SILVER},
                    "object": {"key": arquivo_real_mais_recente}
                  }
                }
              ]
            }
            # Dispara a função principal simulando a nuvem
            lambda_handler(evento_s3_mock, context=None)
        else:
            print(f"⚠️ Aviso: Nenhum arquivo encontrado em s3://{BUCKET_SILVER}/streaming_processado/.")
            print("Você precisa rodar o pipeline_silver_streaming.py pelo menos uma vez para ter arquivos de teste!")
    except Exception as e:
        print(f"Erro ao buscar arquivos para simulação: {e}")
