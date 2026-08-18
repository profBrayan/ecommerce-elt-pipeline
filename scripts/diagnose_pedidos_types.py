"""Percorre toda a API de pedidos e reporta, por coluna, quais tipos Python
aparecem — para achar a inconsistência de tipo que quebra pyarrow.Table.from_pylist."""
from collections import defaultdict

from dagster_project.resources.http import SalesAPIClient

COLUMNS = [
    "pedido_id", "cliente_id", "produto_id", "quantidade",
    "valor_unitario", "status", "data_pedido", "updated_at",
]

client = SalesAPIClient()
types_seen: dict[str, set] = defaultdict(set)
samples: dict[str, dict] = defaultdict(dict)
total = 0

for payload in client.iter_pedidos():
    for item in payload.get("data", []):
        total += 1
        for col in COLUMNS:
            val = item.get(col)
            t = type(val).__name__
            types_seen[col].add(t)
            if t not in samples[col]:
                samples[col][t] = val

print(f"Total de registros: {total}\n")
for col in COLUMNS:
    ts = types_seen[col]
    marker = "  <-- MISTO" if len(ts) > 1 else ""
    print(f"{col}: {ts}{marker}")
    if len(ts) > 1:
        for t, v in samples[col].items():
            print(f"    {t}: {v!r}")
