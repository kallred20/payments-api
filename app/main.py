from fastapi import FastAPI
from app.db import get_conn

app = FastAPI()

@app.get("/db-health")
def db_health():
    try:
        conn = get_conn()
        conn.run("SELECT 1")
        conn.close()
        return {"db": "ok"}
    except Exception as e:
        return {"db": "error", "detail": str(e)}
