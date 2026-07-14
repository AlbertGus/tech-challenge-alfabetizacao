📚 Data Lakehouse Educacional: Monitoramento da Alfabetização no Brasil

1. Contexto do Problema e Desafio Educacional

A alfabetização na idade certa é o pilar fundamental para o desenvolvimento cognitivo e social de uma criança, ditando a sua trajetória acadêmica futura. No Brasil, o acompanhamento deste indicador (muitas vezes mensurado por avaliações como o Saeb) é um desafio logístico e analítico gigantesco devido à dimensão continental do país e à heterogeneidade das redes de ensino.

O uso do Indicador de Alfabetização permite que gestores identifiquem rapidamente municípios e escolas que estão abaixo da meta estabelecida. O objetivo deste projeto é construir uma Plataforma de Dados Híbrida (Batch e Streaming) escalável, de baixo custo e com alta governança, capaz de centralizar os dados educacionais brutos e transformá-los em insights acionáveis para a tomada de decisão pública.

2. Arquitetura Proposta e Fluxo de Dados

A solução foi arquitetada utilizando o padrão Medalhão (Medallion Architecture) em ambiente de nuvem AWS, priorizando serviços gerenciados (Serverless) e armazenamento em Object Store para otimização de custos (FinOps).

2.1. Fluxo de Dados

Ingestão Híbrida:

Batch: Carga histórica de dados estruturados (Municípios, Metas Nacionais e Estaduais) provenientes da Base dos Dados ingeridos via scripts Python agendados.

Streaming (Tempo Real): Eventos de avaliações de alunos gerados em tempo real, enviados diretamente via API (S3 Direct Push).

Bronze Layer (Raw): Dados brutos pousam no Amazon S3 em seus formatos originais (.csv e .json), particionados por data de ingestão.

Silver Layer (Trusted): Scripts de processamento extraem os dados da Bronze, aplicam limpeza de encoding, tratam nulos, validam chaves (Data Quality/DLQ) e convertem os arquivos para o formato colunar otimizado Parquet.

Gold Layer (Analytics / Event-Driven): A Camada Gold é atualizada por meio de uma Arquitetura Orientada a Eventos. Quando um arquivo processado "pinga" na camada Silver, um evento (AWS S3 Trigger/EventBridge) aciona automaticamente o pipeline Gold (lambda_handler), recalculando os KPIs em tempo real. Os dados agregados são consumidos via Amazon Athena.

2.2. Diagrama da Pipeline

<img width="5277" height="4622" alt="Data Lakehouse Pipeline" src="https://github.com/user-attachments/assets/7e18ee3e-a526-4245-b5dd-f9b8f9370965" />


3. Tecnologias Utilizadas

Linguagem: Python 3 (Pandas para processamento em memória, Boto3 para integração cloud).

Armazenamento (Data Lake): Amazon S3 (Econômico, durável, ideal para arquiteturas particionadas).

Processamento: Python simulando instâncias de AWS Lambda (Orientado a Eventos) e AWS Glue.

Motor de Consultas: Amazon Athena (Serverless, cobra apenas por terabyte escaneado).

Formatos de Arquivo: JSON/CSV (Ingestão) e Parquet (Armazenamento analítico).

4. Decisões Arquiteturais e Trade-offs

Arquitetura Orientada a Eventos (Event-Driven) vs Agendamento (Scheduler): Na camada Gold, para o processamento das avaliações em tempo real, optamos por um design orientado a eventos em vez de rotinas em Batch (Cron). Assim que o dado é tratado na Silver, um "Trigger" do S3 dispara a função AWS Lambda da Gold. O Trade-off: Custo e Latência reduzidos, pois a máquina (Lambda) só é ativada se houver dados novos, processando a informação instantaneamente sem ciclos ociosos de servidor.

🛡️ FinOps na Prática (Simulação da Função Lambda): Para manter o projeto dentro das rígidas diretrizes de custo zero, a função Lambda não foi instanciada diretamente no painel da AWS Cloud para evitar cobranças indevidas de execução. No entanto, o código da Camada Gold (pipeline_gold_streaming.py) foi encapsulado exatamente no formato lambda_handler(event, context) exigido pela Amazon. Adicionamos ao final do script um simulador de gatilhos (Mock Event) que procura a última atualização na Silver e dispara a função automaticamente, provando o sucesso técnico do conceito arquitetural.

Batch vs. Streaming: Utilizamos uma abordagem híbrida Lambda/Kappa adaptada. Os dados de metas e diretórios (que mudam anualmente) utilizam Batch. Já as notas dos alunos necessitam de Streaming para identificação imediata de anomalias na infraestrutura de provas.

Data Lake vs. Data Warehouse: Optamos pela construção de um Data Lakehouse (Amazon S3 + Athena) em vez de um DWH tradicional (como o Amazon Redshift). Trade-off: Perdemos milissegundos de performance em queries hiper-complexas, mas reduzimos os custos de infraestrutura em quase 100%, não havendo clusters ligados ociosamente.

Streaming Engine (Kinesis vs S3 Push): Trade-off Crítico: Inicialmente projetado com Amazon Kinesis Data Firehose. Contudo, devido a restrições de Billing e Free Tier, pivotamos a arquitetura para o Direct-to-S3 Micro-batching. A lógica de buffering e particionamento (ano/mês/dia/hora) foi reconstruída no produtor Python. Perdemos a resiliência do Kinesis, mas atingimos o objetivo FinOps de "Custo Zero".

5. Governança, Monitoramento e FinOps

Otimização de Custos em Cloud (FinOps)

A arquitetura foi desenhada com mentalidade Cost-Aware:

Uso de Parquet: Na transição da Bronze para a Silver, os arquivos são compactados em .parquet com compressão Snappy, reduzindo o tamanho em disco e o volume escaneado pelo Athena.

Particionamento Lógico: Os dados no S3 estão estruturados por ano=XXXX/mes=XX. Queries analíticas consultam partições específicas, economizando processamento computacional.

Governança e Qualidade de Dados (Data Quality)

Quarentena (DLQ): Regras de validação verificam a integridade das chaves (ex: id_municipio). Registros malformados no Streaming não quebram o pipeline; são roteados para um bucket de Quarentena (/quarentena/streaming_falhas/) para futura auditoria.

Padronização: Correção de encodings (ex: PÃºblica para Pública) e tratamento de valores nulos matemáticos nas proporções.

Monitoramento da Pipeline

O pipeline integra um Módulo de Observabilidade Customizado. A cada execução do pipeline Gold, um artefato JSON de telemetria é gerado com:

Volume de linhas processadas.

Latência total do script (segundos).

Status final e captura de exceções (Alertas de erro).

Esses logs são armazenados no próprio Data Lake, permitindo que Dashboards monitorem a saúde da operação.

6. Aplicação em Inteligência Artificial (IA)

A consolidação da Camada Gold padronizada abre portas gigantescas para modelos de IA impactarem a educação pública:

Modelos de Predição de Alfabetização: Cruzando a Camada Gold com dados socioeconômicos do IBGE (Renda, Saneamento), modelos de Machine Learning (como XGBoost) podem prever com meses de antecedência quais municípios correm risco de não atingir a meta, permitindo intervenção prévia.

Análise de Desigualdade e Clusterização: Algoritmos não supervisionados (como K-Means) podem agrupar municípios com desafios semelhantes, criando "Clusters de Vulnerabilidade Educacional" que facilitam o desenho de políticas públicas sob medida.

Alocação de Recursos (Otimização): IA baseada em dados pode sugerir exatamente onde o Ministério da Educação deve injetar verbas do FUNDEB para obter o maior Retorno sobre Investimento (ROI) nos índices de alfabetização.
