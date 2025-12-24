import os
from pathlib import Path 
import pandas as pd 
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch

#Carregando variáveis de ambiente
load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[3]
SILVER_DIR = BASE_DIR/"data"/"silver"       #Dados da silver

DB_CONFIG = {
    "host": "localhost",
    "port": os.getenv("POSTGRES_PORT", 5432),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

#Estabelecendo conexão
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

#Carregando Silver em banco
def load_recently_played():
    df = pd.read_csv(
        SILVER_DIR/ "recently_played.csv",
        parse_dates =["played_at"]
        )
        
    # 🔧 Corrige datas inválidas
    df["album_release_date"] = pd.to_datetime(
        df["album_release_date"], errors="coerce"
    ).dt.date

    # 🔧 Converte NaN em None (NULL no Postgres)
    df = df.where(pd.notnull(df), None)

    #Inserindo informação na tabela do banco - se informação já existir, ingnora
    sql = """
        INSERT INTO silver.recently_played(
        played_at,
        track_id,
        track_name,
        duration_ms,
        popularity,
        explicit,
        album_id,
        album_name,
        album_release_date,
        artist_id,
        artist_name,
        load_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(played_at, track_id) DO NOTHING
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(
                cur,
                sql,
                df[[
                    "played_at",
                    "track_id",
                    "track_name",
                    "duration_ms",
                    "popularity",
                    "explicit",
                    "album_id",
                    "album_name",
                    "album_release_date",
                    "artist_id",
                    "artist_name",
                    "load_date"
                    ]].values.tolist()
            )
        
        conn.commit()
    
    print(f"✅ recently_played processada ({len(df)} linhas lidas do CSV)")


def run_load_silver_to_db():

    print("🗄️ Iniciando carga da SILVER no PostgreSQL")
    load_recently_played()
    print("✅ Carga da SILVER no PostgreSQL finalizada")
