import pandas as pd
import boto3
import io
import json
from datetime import datetime

# ==============================================================================
# CONFIGURAÇÃO DE SEGURANÇA AWS
# ==============================================================================
AWS_ACCESS_KEY = '#######'
AWS_SECRET_KEY = '#######'
REGIAO_AWS = 'us-east-1'

BUCKET_BRONZE = 'c-bronze'
BUCKET_SILVER = 'c-silver'

print("1. A ligar à AWS S3 (Pipeline Silver para Streaming)...")
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGIAO_AWS
)


def processar_streaming_para_silver():
    try:
        # ======================================================================
        # 1. EXTRAÇÃO: Ler os ficheiros JSON da Camada Bronze
        # ======================================================================
        print("\n2. A procurar novos eventos de streaming na Camada Bronze...")

        # O S3 lista todos os ficheiros que estão dentro da pasta 'streaming/'
        resposta = s3_client.list_objects_v2(Bucket=BUCKET_BRONZE, Prefix='streaming/')

        if 'Contents' not in resposta:
            print("Nenhum evento novo de streaming encontrado para processar.")
            return

        lista_ficheiros = [obj['Key'] for obj in resposta['Contents'] if obj['Key'].endswith('.json')]
        print(f"-> Encontrados {len(lista_ficheiros)} ficheiros JSON. A iniciar leitura...")

        eventos = []
        for ficheiro in lista_ficheiros:
            # Lê o ficheiro JSON do S3
            obj = s3_client.get_object(Bucket=BUCKET_BRONZE, Key=ficheiro)
            conteudo_json = json.loads(obj['Body'].read().decode('utf-8'))
            eventos.append(conteudo_json)

        # Converte a lista de JSONs para um DataFrame do Pandas
        df_streaming = pd.DataFrame(eventos)

        # ======================================================================
        # 2. TRANSFORMAÇÃO: Limpeza e Tipagem (Esquema da Camada Silver)
        # ======================================================================
        print("\n3. A aplicar regras de Qualidade e Tipagem (Schema Enforcement)...")

        # A. Garantir que o ID do Município é do tipo String (Texto)
        # (Para evitar aquele erro HIVE_BAD_DATA no Athena)
        df_streaming['id_municipio'] = df_streaming['id_municipio'].astype(str)

        # B. Garantir que a nota é um número decimal (Float)
        df_streaming['nota_avaliacao'] = pd.to_numeric(df_streaming['nota_avaliacao'], errors='coerce')

        # C. Adicionar metadados da engenharia (Quando é que este dado foi processado?)
        df_streaming['processado_em_silver'] = datetime.utcnow().isoformat()

        # Mostra uma amostra de como os dados ficaram bonitos e tabulados
        print("\n--- Amostra dos Dados Tratados ---")
        display(df_streaming.head())

        # ======================================================================
        # 3. CARGA (LOAD): Converter para Parquet e enviar para a Silver
        # ======================================================================
        print("\n4. A converter para Parquet e a enviar para a Camada Silver...")

        # Gerar um nome único para este lote (micro-batch) usando a data/hora atual
        agora_str = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        nome_ficheiro_parquet = f"streaming_processado/lote_{agora_str}.parquet"

        buffer_parquet = io.BytesIO()
        df_streaming.to_parquet(buffer_parquet, index=False, engine='pyarrow')

        s3_client.put_object(
            Bucket=BUCKET_SILVER,
            Key=nome_ficheiro_parquet,
            Body=buffer_parquet.getvalue()
        )

        print(f"\n✅ SUCESSO! Lote de {len(df_streaming)} eventos guardado na Silver!")
        print(f"Caminho: s3://{BUCKET_SILVER}/{nome_ficheiro_parquet}")
        print("FinOps aplicado: Transformamos milhares de JSONs caros num único Parquet barato!")

    except Exception as e:
        print(f"\n❌ ERRO NO PIPELINE SILVER: {e}")


# Executar a função
processar_streaming_para_silver()