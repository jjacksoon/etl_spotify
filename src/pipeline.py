import json
from pathlib import Path

from src.extract.spotify.user_recently_played import get_recently_played
from src.load.raw.raw_loader import save_recently_played_raw
from src.transform.silver.silver_recently_played import run_silver
from src.transform.gold.gold_recently_played import run_gold
from src.load.db.create_tables import create_tables
from src.load.db.load_silver_to_db import run_load_silver_to_db
from src.load.db.load_gold_to_db import run_load_gold_to_db



BASE_DIR = Path(__file__).resolve().parent.parent
TOKEN_PATH = BASE_DIR / "token.json"

def load_access_token() -> str:
    with open(TOKEN_PATH, encoding="utf-8") as f:
        return json.load(f)["access_token"]


def run_pipeline():
    print("🚀 Pipeline iniciado")

   # 0. Garantir estrutura do banco
    create_tables()
    print("🗄️ Estrutura do banco garantida")

    # 1. Extract + Raw
    token = load_access_token()
    data = get_recently_played(token, limit=10)
    raw_path = save_recently_played_raw(data)
    print(f"📥 RAW gerada em {raw_path}")

    # 2. Silver
    run_silver()
    print("🥈 SILVER gerada com sucesso")

    # 3. Gold (CSV)
    run_gold()
    print("🥇 GOLD gerada com sucesso")

    # 4. Load Silver → PostgreSQL
    run_load_silver_to_db()

    # 4. Load Gold → PostgreSQL
    run_load_gold_to_db()

    print("✅ Pipeline finalizada com sucesso")


if __name__ == "__main__":
    run_pipeline()
