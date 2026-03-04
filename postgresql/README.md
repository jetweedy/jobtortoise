
# PostGreSQL

## Docker

### Setup

```
pip install sqlalchemy psycopg[binary]

docker compose up -d
docker compose ps
docker logs -f pg-dev   # Ctrl+C to stop viewing logs
```

### Connecting / Smoke Test

```
from jetDB import db_execute

PG_DSN = "postgresql+psycopg://appuser:appsecret@localhost:5432/appdb"

# create a table
print(db_execute(PG_DSN, """
CREATE TABLE IF NOT EXISTS testTable (
  id BIGSERIAL PRIMARY KEY,
  rantext TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
""", return_rows=False))

# insert a row
print(db_execute(PG_DSN,
    "INSERT INTO users (rantext) VALUES (:rantext) RETURNING id, email",
    params={"rantext":"12345"},
    return_rows=True))

# select
print(db_execute(PG_DSN,
    "SELECT id, rantext FROM users testTable BY id DESC"))
```
