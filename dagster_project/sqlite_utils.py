"""Leitura de tabelas SQLite grandes em batches, sem materializar a tabela
inteira em memória (nem em pandas)."""
from __future__ import annotations

import sqlite3
from typing import Iterator


def iter_sqlite_batches(
    sqlite_path: str, table_name: str, batch_size: int
) -> Iterator[list[dict]]:
    conn = sqlite3.connect(sqlite_path)
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        columns = [d[0] for d in cursor.description]
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            yield [dict(zip(columns, row)) for row in batch]
    finally:
        conn.close()
