from __future__ import annotations
import pydantic
import pypika as pp
import pypika.queries as ppq
from typing import Any, Type, Iterable

from arorm.pg_driver import PGExecutor
from arorm.key import Key, generate_tsid
from arorm.exceptions import (
    TableDoesNotExist,
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
    def execute_query(cls, query: str, *args: Any) -> list:
        return PGExecutor().execute(query, args)
    
    @classmethod
    def _check_table_existance(cls, table_name: str) -> bool:
        res = cls.execute_query(
            f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE  table_name = %s
                );
            """,
            table_name,
        )
        return len(res) > 0 and res[0][0]

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
            col_type = {
                "integer": "INT",
                "string": "VARCHAR(255)",
                "boolean": "BOOLEAN",
            }[col_type]
            
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
    def _parse_model[T: "BaseModel"](cls: Type[T], values: list[Any]) -> T:
        obj_dict = {k: v for k, v in zip(cls.model_fields, values)}
        return cls.model_validate(obj_dict)

    @classmethod
    def create_table(cls) -> None:
        if not cls._get_config_value("allow_create_table"):
            raise CreateTableNotAllowed
        
        if cls._check_table_existance(cls.__name__.lower()):
            return

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
        
        if not cls._check_table_existance(cls.__name__.lower()):
            raise TableDoesNotExist(cls.__name__.lower())

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

        return cls._parse_model(obj[0])

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

        return [cls._parse_model(obj) for obj in result]

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
