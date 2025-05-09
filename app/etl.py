from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path
import os
import pandas as pd
import pandera as pa
from schema_crm import schema_zeus
import duckdb


def load_settings():
    """Carrega as configurações a partir de variáveis de ambiente"""
    path_env = Path.cwd() / '.env'
    load_dotenv(dotenv_path=path_env)

    settings = {
        'db_host': os.getenv('host'),
        'db_user': os.getenv('usuario'),
        'db_pass': os.getenv('senha'),
        'db_name': os.getenv('bd'),
        'db_port': os.getenv('port')
    }
    return settings

@pa.check_output(schema_zeus())
def extrair_do_sql(query:str) -> pd.DataFrame:
    # Settings da conexão
    settings = load_settings()

    # String de conexxão
    database_strings = f'postgresql://{settings['db_user']}:{settings['db_pass']}@{settings['db_host']}:{settings['db_port']}/{settings['db_name']}'

    # Engine de conexão
    engine = create_engine(database_strings)

    with engine.connect() as conn, conn.begin():
        df = pd.read_sql_query(query, conn)

    return df


def load_to_duckdb(df: pd.DataFrame, table_name: str, db_file: str = 'my_duckdb.db'):

    # Conectar ao DuckDB. Se o arquivo não existri, ele será criado.
    con = duckdb.connect(database=db_file, read_only=False)

    # Registrar o DataFrame como uma tabela Temporária
    con.register('df_temp', df)

    # Utilizar o SQL para inserir os dados da tabela temporária em uma tabela
    # Se a tabela já existir, substitui.
    con.execute(f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_temp')

    # Fechando a conexão
    con.close()




if __name__ == "__main__":
    query = """ SELECT * 
                FROM public."Tb_Zeus"; """
    dados = extrair_do_sql(query=query)
    schema_crm = pa.infer_schema(dados)

    with open('schema_crm.py', 'w', encoding='utf-8') as arquivo:
        arquivo.write(schema_crm.to_script())

    load_to_duckdb(df=dados, table_name="Tb_Tabela_Teste")

