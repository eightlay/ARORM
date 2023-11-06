from __future__ import annotations
import pydantic
import pypika as pp
import pypika.queries as ppq
from typing import Any, Type, Iterable

from arorm.key import Key, generate_tsid
from arorm.exceptions import (
    DropTableNotAllowed, 
    ExtraFieldNotAllowed,
    CreateTableNotAllowed, 
)


class BaseModel(pydantic.BaseModel):
    class Config:
        extra = "forbid"
        
    class DefaultARORMConfig:
        allow_create_table = True
        allow_drop_table = True
        allow_extra_query_fields = False

    id: Key = pydantic.Field(default_factory=generate_tsid)

    @classmethod
    def execute_query(cls, query: str) -> list:
        # TODO: implement
        print(query)
        return []

    @classmethod
    def get_pypika_table(cls) -> pp.Table:
        return pp.Table(cls.__name__.lower())

    @staticmethod
    def translate_pydantic_to_pypika(
        pydantic_schema: dict[str, Any],
    ) -> list[pp.Column]:
        result: list[pp.Column] = []

        for col_name in pydantic_schema["properties"]:
            # TODO: handle anyOf
            col_type = pydantic_schema["properties"][col_name]["type"]
            nullable = col_name not in pydantic_schema["required"]

            # TODO: support more column settings
            result.append(pp.Column(col_name, col_type, nullable=nullable))

        return result
    
    @classmethod
    def _get_config_value(cls, name: str) -> Any:
        cfg = getattr(cls, "ARORMConfig", cls.DefaultARORMConfig)
        val = getattr(cfg, name, getattr(cls.DefaultARORMConfig, name))
        return val

    @classmethod
    def create_table(cls) -> None:
        if not cls._get_config_value("allow_create_table"):
            raise CreateTableNotAllowed

        schema = cls.model_json_schema()

        table = cls.get_pypika_table()
        pypika_columns = cls.translate_pydantic_to_pypika(schema)

        query: ppq.CreateQueryBuilder = pp.Query.create_table(table).columns(
            *pypika_columns
        )

        cls.execute_query(query.get_sql())

    @classmethod
    def drop_table(cls) -> None:
        if not cls._get_config_value("allow_drop_table"):
            raise DropTableNotAllowed

        table = cls.get_pypika_table()
        query = pp.Query.drop_table(table)
        cls.execute_query(query.get_sql())

    @classmethod
    def get[T: "BaseModel"](cls: Type[T], key: Key) -> T | None:
        table = cls.get_pypika_table()
        query: ppq.QueryBuilder = (
            pp.Query.from_(table).where(table.id == key).select("*")
        )
        obj = cls.execute_query(query.get_sql())

        if len(obj) == 0:
            return None

        return cls.model_validate(obj[0])

    def save(self) -> None:
        self.save_record(self)

    @classmethod
    def save_record[T: "BaseModel"](cls: Type[T], record: T) -> None:
        table = cls.get_pypika_table()

        obj = cls.get(record.id)

        if obj is None:
            query: ppq.QueryBuilder = pp.Query.into(table).insert(
                *record.model_dump().values()
            )
        else:
            query: ppq.QueryBuilder = pp.Query.update(table).where(
                table.id == record.id
            )

            for key, val in record.model_dump().items():
                query = query.set(key, val)

        cls.execute_query(query.get_sql())

    @classmethod
    def all[T: "BaseModel"](cls: Type[T]) -> list[T]:
        return cls.filter()

    @classmethod
    def filter[T: "BaseModel"](cls: Type[T], **kwargs: Any) -> list[T]:
        cls._validate_query_fields(kwargs.keys())

        table = cls.get_pypika_table()
        query: ppq.QueryBuilder = pp.Query.from_(table)

        for key, val in kwargs.items():
            query = query.where(getattr(table, key) == val)

        query = query.select("*")
        result = cls.execute_query(query.get_sql())

        return [cls.model_validate(obj) for obj in result]

    @classmethod
    def delete(cls, **kwargs: Any) -> None:
        cls._validate_query_fields(kwargs.keys())

        table = cls.get_pypika_table()
        query: ppq.QueryBuilder = pp.Query.from_(table)

        for key, val in kwargs.items():
            
            
            query = query.where(getattr(table, key) == val)

        query = query.delete()

        cls.execute_query(query.get_sql())

    @classmethod
    def _validate_query_fields(cls, fields: Iterable[str]) -> None:
        if cls._get_config_value("allow_extra_query_fields"):
            return

        for field in fields:
            if field not in cls.model_fields:
                raise ExtraFieldNotAllowed(field)
