import json
import time
import random
import uuid
import boto3
from datetime import datetime

# ==============================================================================
# CONFIGURAÇÃO DE SEGURANÇA AWS E JUSTIFICATIVA FINOPS (DIRECT-TO-S3)
# ==============================================================================
# TRADE-OFF ARQUITETURAL:
# Embora o Amazon Kinesis Firehose seja o padrão gerido para streaming na AWS,
# a documentação oficial confirma que ele NÃO faz parte do AWS Free Tier.
# Para respeitar as diretrizes de FinOps e manter o custo de infraestrutura
# próximo a zero, a arquitetura foi pivotada para "Direct-to-S3 Push".
# Simulamos o comportamento do Firehose particionando os dados via código
# diretamente no Data Lake.

AWS_ACCESS_KEY = '######'
AWS_SECRET_KEY = '#######'
REGIAO_AWS = 'us-east-1'

BUCKET_BRONZE = 'c-bronze'

print("1. Conectando ao Amazon S3 (Modo Ingestão Contínua - FinOps)...")
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGIAO_AWS
)


def gerar_evento_streaming():
    """Gera um evento fictício de uma nova medição educacional."""
    municipios = ['1100015', '1100023', '3550308', '3304557', '4106902']
    dado_corrompido = random.random() < 0.10  # 10% de chance de erro
    agora = datetime.utcnow()

    evento = {
        "id_evento": str(uuid.uuid4()),
        "timestamp": agora.isoformat(),
        "datetime": agora.strftime('%Y-%m-%d %H:%M:%S'),
        # Se corrompido, gera valores nulos para a Quarentena tratar (DLQ)
        "id_municipio": random.choice(municipios) if not dado_corrompido else None,
        "id_aluno": random.randint(1000, 99999),
        "nota_avaliacao": round(random.uniform(300.0, 900.0), 2) if not dado_corrompido else None,
        "status_medicao": "CONCLUIDA"
    }
    return evento


print("🚀 INICIANDO INGESTÃO DE STREAMING DIRETO (S3 PUSH)...\n")
print("Simulando envio de eventos em tempo real diretamente para o Data Lake.")

try:
    while True:
        dados_evento = gerar_evento_streaming()
        id_evento = dados_evento['id_evento']
        payload_json = json.dumps(dados_evento)

        # Particionamento Inteligente de Tempo (Simulando o Kinesis)
        hoje = datetime.utcnow()
        particoes_tempo = f"ano={hoje.year}/mes={hoje.month:02d}/dia={hoje.day:02d}/hora={hoje.hour:02d}"

        # Separação de DLQ (Quarentena) vs Sucesso na borda da arquitetura
        if dados_evento.get('id_municipio') is None or dados_evento.get('nota_avaliacao') is None:
            caminho_s3 = f"quarentena/streaming_falhas/{particoes_tempo}/erro_{id_evento}.json"
            status_print = "⚠️ [QUARENTENA]"
        else:
            caminho_s3 = f"streaming/{particoes_tempo}/evento_{id_evento}.json"
            status_print = "✅ [S3 STREAMING OK]"

        # Envio direto para o S3
        s3_client.put_object(
            Bucket=BUCKET_BRONZE,
            Key=caminho_s3,
            Body=payload_json,
            ContentType='application/json'
        )

        print(f"{status_print} Evento salvo em: s3://{BUCKET_BRONZE}/{caminho_s3}")

        # Espera aleatória (1 a 3 segundos) para não estourar limite de chamadas PUT gratuitas
        time.sleep(random.uniform(1, 3))

except KeyboardInterrupt:
    print("\n🛑 Simulação de Streaming interrompida pelo Engenheiro de Dados.")
except Exception as e:
    print(f"\n❌ ERRO NA INGESTÃO: {e}")