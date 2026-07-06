import pandas as pd
import boto3
import io

# ==============================================================================
# CONFIGURAÇÃO DE SEGURANÇA AWS
# ==============================================================================
# Substitua pelas chaves que acabou de gerar na AWS
AWS_ACCESS_KEY = '#####'
AWS_SECRET_KEY = '######'
REGIAO_AWS = 'us-east-1'

BUCKET_BRONZE = 'c-bronze'
BUCKET_SILVER = 'c-silver'

print("1. A ligar à AWS S3...")
# Inicializa o cliente do S3 com as suas credenciais
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGIAO_AWS
)

def processar_e_enviar_parquet(nome_ficheiro_csv):
    print(f"\nA processar: {nome_ficheiro_csv}...")

    try:
        # 1. LER DA CAMADA BRONZE (Extração)
        obj_bronze = s3_client.get_object(Bucket=BUCKET_BRONZE, Key=nome_ficheiro_csv)
        df = pd.read_csv(obj_bronze['Body'])

        # 2. LIMPEZA E TRANSFORMAÇÃO (Camada Silver)
        # Correção do erro de encoding (PÃºblica -> Pública)
        df = df.replace('PÃºblica', 'Pública', regex=True)

        # Preenchimento de valores nulos (NaN) com 0 nas colunas de proporção
        colunas_proporcao = [col for col in df.columns if 'proporcao' in col]
        if colunas_proporcao:
            df[colunas_proporcao] = df[colunas_proporcao].fillna(0.0)

        # Remover duplicados
        df = df.drop_duplicates()

        # 3. CONVERTER PARA PARQUET E ENVIAR PARA A SILVER (Carga / FinOps)
        # Em vez de guardar no PC, guardamos na memória temporária e enviamos direto
        buffer_parquet = io.BytesIO()
        df.to_parquet(buffer_parquet, index=False, engine='pyarrow')

        # Nome do novo ficheiro (troca .csv por .parquet)
        nome_ficheiro_parquet = nome_ficheiro_csv.replace('.csv', '.parquet')

        s3_client.put_object(
            Bucket=BUCKET_SILVER,
            Key=nome_ficheiro_parquet,
            Body=buffer_parquet.getvalue()
        )
        print(f"[SUCESSO] Guardado na Silver: s3://{BUCKET_SILVER}/{nome_ficheiro_parquet}")

    except Exception as e:
        print(f"[ERRO] Falha ao processar {nome_ficheiro_csv}: {e}")

# ==============================================================================
# EXECUÇÃO DO PIPELINE
# ==============================================================================
# Lista exata dos ficheiros que fez upload para a Bronze
ficheiros_bronze = [
    'br_inep_avaliacao_alfabetizacao_municipio.csv',
    'br_inep_avaliacao_alfabetizacao_uf.csv',
    'br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv',
    'br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv',
    'br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv',
    'br_bd_diretorios_brasil_uf.csv'
]

print("2. A iniciar pipeline de transformação...")
for ficheiro in ficheiros_bronze:
    processar_e_enviar_parquet(ficheiro)

print("\n🚀 MISSÃO SILVER CONCLUÍDA! O Data Lake está otimizado.")