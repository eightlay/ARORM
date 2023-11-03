import pypika as pp
from typing import Any, Type
from pypika.queries import CreateQueryBuilder, QueryBuilder

from arorm.key import Key
from arorm.model import BaseModel


class BaseDAO[Model: BaseModel]:
    model: Type[Model]

    @classmethod
    def execute_query(cls, query: str) -> list[Model]:
        # TODO: implement
        print(query)
        return []

    @classmethod
    def create_table(cls) -> None:
        schema = cls.model.model_json_schema()

        table = cls.model.get_pypika_table()
        pypika_columns = cls.model.translate_pydantic_to_pypika(schema)

        query: CreateQueryBuilder = pp.Query.create_table(table).columns(
            *pypika_columns
        )

        cls.execute_query(query.get_sql())

    @classmethod
    def drop_table(cls) -> None:
        table = cls.model.get_pypika_table()
        query = pp.Query.drop_table(table)
        cls.execute_query(query.get_sql())

    @classmethod
    def get(cls, key: Key) -> Model | None:
        table = cls.model.get_pypika_table()
        query: QueryBuilder = pp.Query.from_(table).where(table.id == key).select("*")
        obj = cls.execute_query(query.get_sql())

        if len(obj) == 0:
            return None

        return cls.model.model_validate(obj[0])

    @classmethod
    def save(cls, record: Model) -> None:
        table = cls.model.get_pypika_table()

        obj = cls.get(record.id)

        if obj is None:
            query: QueryBuilder = pp.Query.into(table).insert(
                *record.model_dump().values()
            )
        else:
            query: QueryBuilder = pp.Query.update(table).where(table.id == record.id)

            for key, val in record.model_dump().items():
                query = query.set(key, val)

        cls.execute_query(query.get_sql())

    @classmethod
    def all(cls) -> list[Model]:
        return cls.filter()

    @classmethod
    def filter(cls, **kwargs: Any) -> list[Model]:
        table = cls.model.get_pypika_table()
        query: QueryBuilder = pp.Query.from_(table)

        for key, val in kwargs.items():
            query = query.where(getattr(table, key) == val)

        query = query.select("*")
        result = cls.execute_query(query.get_sql())

        return [cls.model.model_validate(obj) for obj in result]

    @classmethod
    def delete(cls, **kwargs: Any) -> None:
        table = cls.model.get_pypika_table()
        query: QueryBuilder = pp.Query.from_(table)

        for key, val in kwargs.items():
            query = query.where(getattr(table, key) == val)

        query = query.delete()

        cls.execute_query(query.get_sql())
