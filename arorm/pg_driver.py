import psycopg2
from typing import Any
from threading import Lock

from arorm.config import config


class SingletonMeta(type):
    """
    Thread-safe implementation of Singleton.
    """

    _instances = {}

    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class PGExecutor(metaclass=SingletonMeta):
    """
    We'll use this property to prove that our Singleton really works.
    """

    def __init__(
        self,
    ) -> None:
        self.conn = psycopg2.connect(
            user=config.PG_USER,
            password=config.PG_PASS,
            host=config.PG_HOST,
            port=config.PG_PORT,
            database=config.PG_DB,
        )
        self.cur = self.conn.cursor()

    def execute(self, query: str, args: tuple[Any, ...]) -> list[Any]:
        self.cur.execute(query, args)

        try:
            res = self.cur.fetchall()
        except psycopg2.ProgrammingError:
            res = []

        self.conn.commit()
        return res
