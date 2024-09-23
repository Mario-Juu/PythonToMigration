import pandas as pd
import logging
from sqlalchemy import create_engine

logging.basicConfig(filename='migration.log', level=logging.INFO)
engine = create_engine('dialect+driver://username:password@host:port/database')
logging.info("Conectado. Migração iniciada.")
connection = engine.connect()
logging.info("Tabela selecionada.")
query_origem = "SELECT * FROM tabela_migrada"

df = pd.read_sql(query_origem, engine)
df.to_json('dados_origem.json', index=False)
logging.info("Dados lidos e convertidos.")

df.dropna(inplace=True)
df.drop_duplicates(inplace=True)
logging.info("Retirada de duplicatas e nulos.")

engine_destino = create_engine('dialect+driver://username:password@host:port/database')
logging.info("Conexão com o banco a ser migrado..")

df.to_sql('usuario', engine_destino, if_exists='append',index=False)
logging.info("Dados migrados.")

query_contagem = "SELECT COUNT(*) FROM tabela_migrada"
df_contagem = pd.read_sql(query_contagem, engine_destino)
print(f'Total de registros no destino: {df_contagem.iloc[0,0]}')
df = pd.read_sql(query_contagem, engine)
if df.iloc[0,0] == df_contagem.iloc[0,0]:
    print("Migração concluida com sucesso")
else:
    print("A contagem de registros não bate. Verifique os dados.")

