import pandas as pd
import json

INPUT_FILE = "ipeds_summary_table.csv"

JSON_OUT = "ipeds_summary_table.json"
TXT_OUT = "schools.txt"

# Read CSV
df = pd.read_csv(INPUT_FILE)

# ---- Write JSON (keep full original URLs) ----
records = df.to_dict(orient="records")

with open(JSON_OUT, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2)

# ---- Create schools.txt with only ___ .edu domains ----
domains = (
    df["url"]
    .dropna()
    .str.lower()
    .str.extract(r'([a-zA-Z0-9-]+\.edu)', expand=False)
    .dropna()
    .unique()
)

domains = sorted(domains)

with open(TXT_OUT, "w", encoding="utf-8") as f:
    for d in domains:
        f.write(d + "\n")

print(f"Wrote {len(records)} rows to {JSON_OUT}")
print(f"Wrote {len(domains)} domains to {TXT_OUT}")