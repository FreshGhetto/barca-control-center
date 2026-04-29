import sys
sys.path.insert(0, '.')
from barca_control_center.db_sync import get_db_dsn
import psycopg

dsn = get_db_dsn()
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT conname FROM pg_constraint "
            "WHERE conrelid = 'fact_feature_state'::regclass AND contype = 'c';"
        )
        constraints = [r[0] for r in cur.fetchall()]
        print(f"Constraint trovati: {constraints}")
        for c in constraints:
            cur.execute(f"ALTER TABLE fact_feature_state DROP CONSTRAINT IF EXISTS {c};")
            print(f"Rimosso: {c}")
    conn.commit()
print("Fatto.")

