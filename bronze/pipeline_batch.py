import boto3
import os
from datetime import datetime

# ==============================================================================
# CONFIGURAÇÃO DE SEGURANÇA AWS E BATCH
# ==============================================================================
AWS_ACCESS_KEY = '######'
AWS_SECRET_KEY = '######'
REGIAO_AWS = 'us-east-1'

# O Batch vai alimentar a camada BRONZE com dados históricos estruturados
BUCKET_BRONZE = 'c-bronze'

print("1. Conectando ao Amazon S3 (Modo Ingestão Batch)...")
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGIAO_AWS
)


def executar_ingestao_batch():
    """
    Simula o job de Ingestão Batch periódico (Ex: Rodando toda madrugada às 02:00).
    Lê os arquivos CSV locais (extraídos da Base dos Dados)
    e faz o upload (Push) na Camada Bronze particionada.
    """
    # Lista dos arquivos CSV que compõem a nossa carga histórica de metas e municípios
    arquivos_csv = [
        'br_inep_avaliacao_alfabetizacao_municipio.csv',
        'br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv',
        'br_bd_diretorios_brasil_uf.csv'
    ]

    # Particionamento Lógico de Tempo para FinOps (Organizando o Data Lake)
    hoje = datetime.utcnow()
    particao_batch = f"batch/ano={hoje.year}/mes={hoje.month:02d}/dia={hoje.day:02d}"

    print("\n🚀 INICIANDO CARGA BATCH DOS DADOS HISTÓRICOS...\n")

    for arquivo in arquivos_csv:
        caminho_s3 = f"{particao_batch}/{arquivo}"

        try:
            # Faz o upload do arquivo local para a nuvem AWS S3
            s3_client.upload_file(
                Filename=arquivo,
                Bucket=BUCKET_BRONZE,
                Key=caminho_s3
            )
            print(f"✅ [BATCH OK] {arquivo} ingerido com sucesso em s3://{BUCKET_BRONZE}/{caminho_s3}")
        except FileNotFoundError:
            print(f"⚠️ [AVISO] Arquivo local '{arquivo}' não encontrado. (Apenas simulação no repositório).")
        except Exception as e:
            print(f"❌ [ERRO] Falha ao ingerir {arquivo}: {e}")


# Executa o Job Batch
if __name__ == "__main__":
    executar_ingestao_batch()
    print("\n🏆 Processo Batch concluído e histórico preservado na Bronze!")