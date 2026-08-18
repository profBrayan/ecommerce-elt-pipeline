"""Garante que a leitura de tabelas grandes do SQLite acontece em batches
limitados (nunca todas as linhas de uma vez)."""
import sqlite3

import pytest

from dagster_project.sqlite_utils import iter_sqlite_batches


@pytest.fixture
def sqlite_with_1000_rows(tmp_path):
    db_path = tmp_path / "test.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE itens (id INTEGER PRIMARY KEY, valor REAL)")
    conn.executemany(
        "INSERT INTO itens (id, valor) VALUES (?, ?)",
        [(i, i * 1.5) for i in range(1000)],
    )
    conn.commit()
    conn.close()
    return str(db_path)


def test_batches_respect_batch_size(sqlite_with_1000_rows):
    batch_sizes = [len(b) for b in iter_sqlite_batches(sqlite_with_1000_rows, "itens", 250)]
    assert batch_sizes == [250, 250, 250, 250]


def test_batches_cover_all_rows_without_duplicates(sqlite_with_1000_rows):
    seen_ids = []
    for batch in iter_sqlite_batches(sqlite_with_1000_rows, "itens", 300):
        seen_ids.extend(row["id"] for row in batch)
    assert sorted(seen_ids) == list(range(1000))


def test_last_batch_can_be_smaller_than_batch_size(sqlite_with_1000_rows):
    batch_sizes = [len(b) for b in iter_sqlite_batches(sqlite_with_1000_rows, "itens", 300)]
    assert batch_sizes == [300, 300, 300, 100]


def test_iterator_is_lazy_one_batch_at_a_time(sqlite_with_1000_rows):
    """O gerador não deve materializar mais de um batch por vez."""
    gen = iter_sqlite_batches(sqlite_with_1000_rows, "itens", 100)
    first_batch = next(gen)
    assert len(first_batch) == 100
    # a próxima chamada só busca o próximo batch quando explicitamente pedida
    second_batch = next(gen)
    assert len(second_batch) == 100
    assert first_batch[0]["id"] == 0
    assert second_batch[0]["id"] == 100
