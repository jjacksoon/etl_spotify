import os
from pathlib import Path
import pandas as pd 
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

#Carregando variáveis de ambiente
load_dotenv() # lê o arquivo .env e carrega as variáveis para o Python

BASE_DIR = Path(__file__).resolve().parents[3]
GOLD_DIR = BASE_DIR /"data"/"gold"  #dados da camada gold

DB_CONFIG = {
    "host": "localhost",
    "port": os.getenv("POSTGRES_PORT", 5432),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

#Conexão
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

#Carregando Dimensão artista em banco
def load_dim_artist():
    df = pd.read_csv(GOLD_DIR / "dim_artist.csv")

    #inserindo informação na tabela do banco - Se o artista já existir, ignora
    sql = """
        INSERT INTO gold.dim_artist (artist_id, artist_name)
        VALUES (%s,%s)
        ON CONFLICT (artist_id) DO NOTHING
    """

    with get_connection() as conn:      #conecta com o postgresql
        with conn.cursor() as cur:      #cria o cursor
            execute_batch(              #insere varias linhas de uma só vez
                cur,
                sql,
                df[["artist_id", "artist_name"]].values.tolist()
            )
        
        conn.commit()                   #confirma a trasacao no banco para que dados sejam gravados
    print(f"✅ dim_artist carregada ({len(df)} registros)")



#Carregando dimensão album em banco
def load_dim_album():
    df = pd.read_csv(GOLD_DIR / "dim_album.csv")

    # 🔧 Corrige datas inválidas
    df["album_release_date"] = pd.to_datetime(
        df["album_release_date"], errors="coerce"
    ).dt.date

    # 🔧 Converte NaN em None (NULL no Postgres)
    df = df.where(pd.notnull(df), None)

    sql = """
        INSERT INTO gold.dim_album (album_id, album_name, album_release_date, artist_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (album_id) DO NOTHING;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(
                cur,
                sql,
                df[["album_id", "album_name", "album_release_date", "artist_id"]]
                .values.tolist()
            )
        conn.commit()

    print(f"✅ dim_album carregada ({len(df)} registros)")


# Carregando dimensão track em banco
def load_dim_track():
    df = pd.read_csv(GOLD_DIR/"dim_track.csv")

    #Inserindo informações na tabela do banco - se track já existir, ignora
    sql = """
        INSERT INTO gold.dim_track(track_id, track_name, explicit, popularity)
        VALUES(%s,%s,%s,%s)
        ON CONFLICT (track_id) DO NOTHING
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(
                cur,
                sql,
                df[["track_id", "track_name", "explicit", "popularity"]].values.tolist()
            )
        conn.commit()

    print(f"✅ dim_track carregada ({len(df)} registros)")


#Carregando tabela fato em banco
def load_fact_recently_played():
    df = pd.read_csv(
        GOLD_DIR/"fact_recently_played.csv",
        parse_dates = ["played_at"]
        )

    #Inserindo informações na tabela do banco - se faixa ouvida já existir, ingnora
    sql = """
        INSERT INTO gold.fact_recently_played(played_at, track_id, album_id, duration_ms)
        VALUES(%s,%s,%s,%s)
        ON CONFLICT (played_at, track_id) DO NOTHING
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(
                cur,
                sql,
                df[["played_at", "track_id", "album_id", "duration_ms"]].values.tolist()
            )
        conn.commit()

    print(f"✅ fact_recently_played carregada ({len(df)} registros)")


def run_load_gold_to_db():
    print("🗄️ Iniciando carga da GOLD no PostgreSQL")

    load_dim_artist()
    load_dim_album()
    load_dim_track()
    load_fact_recently_played()

    print("✅ Carga da GOLD no PostgreSQL finalizada")
