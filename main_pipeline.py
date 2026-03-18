import os
import json
import pandas as pd

# -----------------------------
# CONFIG (portable paths)
# -----------------------------
BASE = "data"
OUT = "output"
os.makedirs(OUT, exist_ok=True)

# -----------------------------
# SAFE JSON LOADER
# -----------------------------
def load_json_safe(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Skipping file: {path} | Error: {e}")
        return None

# -----------------------------
# SAFE DIRECTORY LISTING
# -----------------------------
def safe_listdir(path):
    try:
        return os.listdir(path)
    except:
        return []

# ==================================================
# 1. AGGREGATED TRANSACTIONS
# ==================================================
rows = []
base = os.path.join(BASE, "aggregated", "transaction", "country", "india", "state")

for state in safe_listdir(base):
    for year in safe_listdir(os.path.join(base, state)):
        for q in safe_listdir(os.path.join(base, state, year)):
            data = load_json_safe(os.path.join(base, state, year, q))
            if not data:
                continue

            txns = data.get("data", {}).get("transactionData") or []
            for t in txns:
                for inst in t.get("paymentInstruments", []):
                    rows.append({
                        "state": state.replace("-", " ").title(),
                        "year": int(year),
                        "quarter": int(q.replace(".json","")),
                        "transaction_type": t.get("name"),
                        "count": inst.get("count"),
                        "amount": inst.get("amount")
                    })

pd.DataFrame(rows).to_csv(f"{OUT}/aggregated_transactions.csv", index=False)

# ==================================================
# 2. MAP TRANSACTIONS
# ==================================================
rows = []
base = os.path.join(BASE, "map", "transaction", "hover", "country", "india", "state")

for state in safe_listdir(base):
    for year in safe_listdir(os.path.join(base, state)):
        for q in safe_listdir(os.path.join(base, state, year)):
            data = load_json_safe(os.path.join(base, state, year, q))
            if not data:
                continue

            hlist = data.get("data", {}).get("hoverDataList") or []
            for d in hlist:
                metric = d.get("metric", [{}])[0]
                rows.append({
                    "state": state.replace("-", " ").title(),
                    "district": d.get("name"),
                    "year": int(year),
                    "quarter": int(q.replace(".json","")),
                    "count": metric.get("count"),
                    "amount": metric.get("amount")
                })

pd.DataFrame(rows).to_csv(f"{OUT}/map_transactions.csv", index=False)

# ==================================================
# 3. AGGREGATED USERS
# ==================================================
rows = []
base = os.path.join(BASE, "aggregated", "user", "country", "india", "state")

for state in safe_listdir(base):
    for year in safe_listdir(os.path.join(base, state)):
        for q in safe_listdir(os.path.join(base, state, year)):
            data = load_json_safe(os.path.join(base, state, year, q))
            if not data:
                continue

            agg = data.get("data", {}).get("aggregated") or {}
            rows.append({
                "state": state.replace("-", " ").title(),
                "year": int(year),
                "quarter": int(q.replace(".json","")),
                "registered_users": agg.get("registeredUsers"),
                "app_opens": agg.get("appOpens")
            })

pd.DataFrame(rows).to_csv(f"{OUT}/aggregated_users.csv", index=False)

# ==================================================
# 4. MAP USERS
# ==================================================
rows = []
base = os.path.join(BASE, "map", "user", "hover", "country", "india", "state")

for state in safe_listdir(base):
    for year in safe_listdir(os.path.join(base, state)):
        for q in safe_listdir(os.path.join(base, state, year)):
            data = load_json_safe(os.path.join(base, state, year, q))
            if not data:
                continue

            hover = data.get("data", {}).get("hoverData") or {}
            for dist, vals in hover.items():
                rows.append({
                    "state": state.replace("-", " ").title(),
                    "district": dist,
                    "year": int(year),
                    "quarter": int(q.replace(".json","")),
                    "registered_users": vals.get("registeredUsers"),
                    "app_opens": vals.get("appOpens")
                })

pd.DataFrame(rows).to_csv(f"{OUT}/map_users.csv", index=False)

# ==================================================
# 5. TOP USERS
# ==================================================
rows = []
base = os.path.join(BASE, "top", "user", "country", "india", "state")

for state in safe_listdir(base):
    for year in safe_listdir(os.path.join(base, state)):
        for q in safe_listdir(os.path.join(base, state, year)):
            data = load_json_safe(os.path.join(base, state, year, q))
            if not data:
                continue

            for d in data.get("data", {}).get("districts") or []:
                rows.append({
                    "state": state.replace("-", " ").title(),
                    "district": d.get("name"),
                    "year": int(year),
                    "quarter": int(q.replace(".json","")),
                    "registered_users": d.get("registeredUsers")
                })

pd.DataFrame(rows).to_csv(f"{OUT}/top_users.csv", index=False)

# ==================================================
# 6. AGGREGATED INSURANCE
# ==================================================
rows = []
base = os.path.join(BASE, "aggregated", "insurance", "country", "india", "state")

for state in safe_listdir(base):
    for year in safe_listdir(os.path.join(base, state)):
        for q in safe_listdir(os.path.join(base, state, year)):
            data = load_json_safe(os.path.join(base, state, year, q))
            if not data:
                continue

            for t in data.get("data", {}).get("transactionData") or []:
                for inst in t.get("paymentInstruments", []):
                    rows.append({
                        "state": state.replace("-", " ").title(),
                        "year": int(year),
                        "quarter": int(q.replace(".json","")),
                        "count": inst.get("count"),
                        "amount": inst.get("amount")
                    })

pd.DataFrame(rows).to_csv(f"{OUT}/aggregated_insurance.csv", index=False)

# ==================================================
# 7. TOP INSURANCE
# ==================================================
rows = []
base = os.path.join(BASE, "top", "insurance", "country", "india", "state")

for state in safe_listdir(base):
    for year in safe_listdir(os.path.join(base, state)):
        for q in safe_listdir(os.path.join(base, state, year)):
            data = load_json_safe(os.path.join(base, state, year, q))
            if not data:
                continue

            for d in data.get("data", {}).get("districts") or []:
                metric = d.get("metric") or {}
                rows.append({
                    "state": state.replace("-", " ").title(),
                    "district": d.get("name"),
                    "year": int(year),
                    "quarter": int(q.replace(".json","")),
                    "count": metric.get("count"),
                    "amount": metric.get("amount")
                })

pd.DataFrame(rows).to_csv(f"{OUT}/top_insurance.csv", index=False)

# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    print("✅ ALL VALID PHONEPE DATASETS EXTRACTED SUCCESSFULLY")