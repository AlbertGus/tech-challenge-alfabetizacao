import pandas as pd
import boto3
import io
from datetime import datetime

# ==============================================================================
# CONFIGURAÇÃO DE SEGURANÇA AWS
# ==============================================================================
AWS_ACCESS_KEY = '#######'
AWS_SECRET_KEY = '########'
REGIAO_AWS = 'us-east-1'

# Nomes dos seus buckets
BUCKET_SILVER = 'c-silver'
BUCKET_GOLD = 'c-gold'

print("1. A ligar à AWS S3...")
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGIAO_AWS
)

def ler_parquet_s3(nome_ficheiro):
    """Função auxiliar para ler ficheiros Parquet diretamente do S3 para o Pandas."""
    print(f"  -> A extrair {nome_ficheiro} da Camada Silver...")
    obj = s3_client.get_object(Bucket=BUCKET_SILVER, Key=nome_ficheiro)
    return pd.read_parquet(io.BytesIO(obj['Body'].read()))

try:
    print("\n2. A EXTRAIR DADOS DA CAMADA SILVER...")
    df_aval = ler_parquet_s3('br_inep_avaliacao_alfabetizacao_municipio.parquet')
    df_meta = ler_parquet_s3('br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.parquet')

    # --------------------------------------------------------------------------
    # NOVIDADE: APLICANDO A QURENTENA (DEAD LETTER QUEUE / DATA QUALITY)
    # --------------------------------------------------------------------------
    print("\n3. AVALIANDO QUALIDADE E ENVIANDO PARA QUARENTENA...")

    # Injetando um dado "sujo" propositalmente apenas para testarmos a quarentena
    linha_suja = pd.DataFrame([{'ano': 2023, 'id_municipio': None, 'taxa_alfabetizacao': None}])
    df_aval = pd.concat([df_aval, linha_suja], ignore_index=True)

    # Regra: Se o ID do municipio for nulo ou a taxa for nula, o dado é inútil.
    mascara_invalidos = df_aval['id_municipio'].isnull() | df_aval['taxa_alfabetizacao'].isnull()

    # Separamos o joio do trigo
    df_quarentena = df_aval[mascara_invalidos].copy()
    df_valido = df_aval[~mascara_invalidos].copy()

    if not df_quarentena.empty:
        # Adicionamos os metadados da falha conforme a sua ideia
        df_quarentena['motivo_falha'] = 'Falta ID do Municipio ou Taxa de Alfabetizacao'
        df_quarentena['data_processamento'] = datetime.now().isoformat()

        # Salvamos na pasta de Quarentena dentro do Bucket Gold
        caminho_quarentena = f"quarentena/erros_avaliacao_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
        buffer_q = io.BytesIO()
        df_quarentena.to_parquet(buffer_q, index=False)
        s3_client.put_object(Bucket=BUCKET_GOLD, Key=caminho_quarentena, Body=buffer_q.getvalue())
        print(f"  -> [ALERTA] {len(df_quarentena)} registos defeituosos enviados para a Quarentena!")
    else:
        print("  -> Dados perfeitos. Nenhum registo enviado para a Quarentena.")

    # --------------------------------------------------------------------------
    # CRUZAMENTO (LÓGICA DE NEGÓCIO)
    # --------------------------------------------------------------------------
    print("\n4. A TRANSFORMAR E CRUZAR OS DADOS (Lógica de Negócio)...")
    df_meta_limpa = df_meta.drop(columns=['taxa_alfabetizacao', 'rede'], errors='ignore')

    # Usamos apenas os dados válidos agora!
    df_gold = pd.merge(df_valido, df_meta_limpa, on=['id_municipio', 'ano'], how='inner')

    # --- CORREÇÃO DE TIPAGEM PARA O ATHENA ---
    # Garantimos que o id_municipio seja tratado como String(texto) no Parquet
    # O astype(int) remove o '.0' caso o pandas tenha transformado em float
    df_gold['id_municipio'] = df_gold['id_municipio'].astype(int).astype(str)

    df_gold['delta_meta_2024'] = df_gold['taxa_alfabetizacao'] - df_gold['meta_alfabetizacao_2024']
    df_gold['status_meta'] = df_gold['delta_meta_2024'].apply(
        lambda x: 'Atingiu a Meta' if x >= 0 else 'Abaixo da Meta'
    )

    # --------------------------------------------------------------------------
    # NOVIDADE: SALVANDO COM PARTICIONAMENTO DE DADOS (FINOPS)
    # --------------------------------------------------------------------------
    print("\n5. A CARREGAR DADOS PARTICIONADOS PARA A CAMADA GOLD (FINOPS)...")

    # Descobrimos quais anos existem na nossa base (ex: 2021, 2023)
    anos_disponiveis = df_gold['ano'].unique()

    for ano_particao in anos_disponiveis:
        # Filtramos a tabela apenas para aquele ano
        df_recorte = df_gold[df_gold['ano'] == ano_particao]

        # Criamos o caminho de pastas com a partição estilo Hive/Athena (ano=XXXX)
        caminho_particionado = f"dados_analiticos/ano={int(ano_particao)}/indicadores.parquet"

        buffer_gold = io.BytesIO()
        df_recorte.to_parquet(buffer_gold, index=False, engine='pyarrow')

        s3_client.put_object(
            Bucket=BUCKET_GOLD,
            Key=caminho_particionado,
            Body=buffer_gold.getvalue()
        )
        print(f"  -> Partição {int(ano_particao)} guardada em: s3://{BUCKET_GOLD}/{caminho_particionado}")

    print("\n🏆 SUCESSO ABSOLUTO! A sua Arquitetura Medalhão agora possui Quarentena e Particionamento!")

except Exception as e:
    print(f"\n[ERRO] O pipeline falhou: {e}")