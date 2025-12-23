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

#Criação das tabelas dimensão

# =========================
# DIM ARTIST
# =========================

create_dim_artist = ("""
    CREATE TABLE IF NOT EXISTS dim_artist(
    artist_id VARCHAR PRIMARY KEY,
    artist_name VARCHAR
    );
""")

# =========================
# DIM ALBUM
# =========================

create_dim_album = ("""
    CREATE TABLE IF NOT EXISTS dim_album(
    album_id VARCHAR PRIMARY KEY,
    album_name VARCHAR,
    album_release_date DATE,
    artist_id VARCHAR REFERENCES dim_artist(artist_id)
    );
""")

# =========================
# DIM TRACK
# =========================

create_dim_track = ("""
    CREATE TABLE IF NOT EXISTS dim_track(
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
    CREATE TABLE IF NOT EXISTS fact_recently_played(
    played_at TIMESTAMP,
    track_id VARCHAR REFERENCES dim_track(track_id),
    album_id VARCHAR REFERENCES dim_album(album_id),
    duration_ms INT,
    PRIMARY KEY (played_at, track_id)
    );
""")


#Executar comandos SQL
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

