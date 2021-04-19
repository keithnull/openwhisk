import sqlite3
from typing import List
from collections import namedtuple

Activation = namedtuple(
    "Activation", ["id", "priority", "name", "start",
                   "end", "duration", "execution_time"]
)


class ActivationStore:

    def __init__(self, filepath):
        self.conn = sqlite3.connect(filepath)
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS activations(
            id,
            name,
            priority,
            created_at,
            started_at,
            ended_at,
            execution_time
        )""")
        self.conn.commit()

    def create_activation(self, activation_id: List[str], created_at: List[str]):
        self.conn.executemany(
            "INSERT INTO activations(id, created_at) values (?, ?)",
            zip(activation_id, created_at)
        )
        self.conn.commit()

    def update_activation(self, activations: List[Activation]):
        self.conn.executemany(
            """
            UPDATE activations SET
                priority = ?,
                name = ?,
                started_at = ?,
                ended_at = ?,
                execution_time = ?
            WHERE id = ?
            """,
            map(lambda a: (a.priority, a.name, a.start, a.end,
                           a.execution_time, a.id), activations)
        )
        self.conn.commit()
