import psycopg2
from dotenv import load_dotenv
import os

#Carregando variáveis de ambiente
load_dotenv() # lê o arquivo .env e carrega as variáveis para o Python

# Criar conexão com o banco
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

#Criar um cursor para executar comandos SQL - percorrer os resultados de uma consulta linha por linha
cursor = conn.cursor()

#Criação do Schema SILVER
create_schema_silver = """
    CREATE SCHEMA IF NOT EXISTS silver
"""

#Criação da tabela Silver recently_played

# =========================
# recently_played
# =========================

create_recently_played = """
    CREATE TABLE IF NOT EXISTS silver.recently_played(
        played_at TIMESTAMP,
        track_id VARCHAR,
        track_name VARCHAR,
        duration_ms INT,
        popularity INT,
        explicit BOOLEAN,
        album_id VARCHAR,
        album_name VARCHAR,
        album_release_date DATE,
        artist_id VARCHAR,
        artist_name VARCHAR,
        load_date DATE,
        CONSTRAINT pk_recently_played PRIMARY KEY (played_at, track_id)
    )
"""

#Criação do Schema GOLD
create_schema_gold = """
    CREATE SCHEMA IF NOT EXISTS gold
"""

#Criação das tabelas dimensão

# =========================
# DIM ARTIST
# =========================

create_dim_artist = ("""
    CREATE TABLE IF NOT EXISTS gold.dim_artist(
    artist_id VARCHAR PRIMARY KEY,
    artist_name VARCHAR
    );
""")

# =========================
# DIM ALBUM
# =========================

create_dim_album = ("""
    CREATE TABLE IF NOT EXISTS gold.dim_album(
    album_id VARCHAR PRIMARY KEY,
    album_name VARCHAR,
    album_release_date DATE,
    artist_id VARCHAR REFERENCES gold.dim_artist(artist_id)
    );
""")

# =========================
# DIM TRACK
# =========================

create_dim_track = ("""
    CREATE TABLE IF NOT EXISTS gold.dim_track(
    track_id VARCHAR PRIMARY KEY,
    track_name VARCHAR,
    explicit BOOLEAN,
    popularity INT
    );
""")



#Criação das tabelas Fato

# =========================
# FACT RECENTLY PLAYED
# =========================

create_fact_recently_played = ("""
    CREATE TABLE IF NOT EXISTS gold.fact_recently_played(
    played_at TIMESTAMP,
    track_id VARCHAR REFERENCES gold.dim_track(track_id),
    album_id VARCHAR REFERENCES gold.dim_album(album_id),
    duration_ms INT,
    PRIMARY KEY (played_at, track_id)
    );
""")


#Executar comandos SQL
def create_tables():
    cursor.execute(create_schema_silver)
    cursor.execute(create_recently_played)
    cursor.execute(create_schema_gold)
    cursor.execute(create_dim_artist)
    cursor.execute(create_dim_album)
    cursor.execute(create_dim_track)
    cursor.execute(create_fact_recently_played)

    #Confirmar alterações
    conn.commit()

    #Fechar cursor e conexão
    cursor.close()
    conn.close()

    print("✅ Tabelas criadas com sucesso")

