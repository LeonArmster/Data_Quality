from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path
import os
import pandas as pd
import pandera as pa
from schema_crm import schema_zeus


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


if __name__ == "__main__":
    query = """ SELECT * 
                FROM public."Tb_Zeus"; """
    dados = extrair_do_sql(query=query)
    schema_crm = pa.infer_schema(dados)

    with open('schema_crm.py', 'w', encoding='utf-8') as arquivo:
        arquivo.write(schema_crm.to_script())

# poetry run python app/etl.py