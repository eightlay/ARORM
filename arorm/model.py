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
    """Base model class which implements Active Record pattern"""

    class Config:
        extra = "forbid"

    class DefaultARORMConfig:
        allow_create_table = True
        allow_drop_table = True
        allow_extra_query_fields = False

    id: Key = pydantic.Field(default_factory=generate_tsid)
    """Object Time-Sorted Unique Identifier"""

    @classmethod
    def _execute_query(cls, query: str, *args: Any) -> list[Any]:
        """Execute query

        Args:
            query (str)
            args (Any): query parameters

        Returns:
            list: query result (empty list if no result is returned)
        """
        return PGExecutor().execute(query, args)

    @classmethod
    def _check_table_existance(cls, table_name: str) -> bool:
        """Check if table exists in the database"""
        res = cls._execute_query(
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
    def _get_pypika_table(cls) -> pp.Table:
        """Get pypika table object

        Returns:
            pp.Table: pypika table object
        """
        return pp.Table(cls.__name__.lower())

    @staticmethod
    def _translate_pydantic_to_pypika(
        pydantic_schema: dict[str, Any],
    ) -> list[pp.Column]:
        """Translate pydantic schema to pypika columns

        Args:
            pydantic_schema (dict[str, Any]): pydantic schema

        Returns:
            list[pp.Column]: list of pypika columns
        """
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
        """Get model config value

        Args:
            name (str): config value name

        Returns:
            Any
        """
        cfg = getattr(cls, "ARORMConfig", cls.DefaultARORMConfig)
        val = getattr(cfg, name, getattr(cls.DefaultARORMConfig, name))
        return val

    @classmethod
    def _parse_model[T: "BaseModel"](cls: Type[T], values: list[Any]) -> T:
        """Parse SQL query result to model object

        Returns:
            Parsed model object
        """
        obj_dict = {k: v for k, v in zip(cls.model_fields, values)}
        return cls.model_validate(obj_dict)

    @classmethod
    def _validate_query_fields(cls, fields: Iterable[str]) -> None:
        """Validate query fields

        Args:
            fields (Iterable[str]): query fields

        Raises:
            ExtraFieldNotAllowed: if extra query fields are not allowed in the ARORMConfig and there are some
        """
        if cls._get_config_value("allow_extra_query_fields"):
            return

        for field in fields:
            if field not in cls.model_fields:
                raise ExtraFieldNotAllowed(field)

    @classmethod
    def create_table(cls) -> None:
        """Create table in the database if it doesn't exist

        Raises:
            CreateTableNotAllowed: if table creation is not allowed in the ARORMConfig
        """
        if not cls._get_config_value("allow_create_table"):
            raise CreateTableNotAllowed

        if cls._check_table_existance(cls.__name__.lower()):
            return

        schema = cls.model_json_schema()

        table = cls._get_pypika_table()
        pypika_columns = cls._translate_pydantic_to_pypika(schema)

        query: ppq.CreateQueryBuilder = pp.Query.create_table(table).columns(
            *pypika_columns
        )

        cls._execute_query(query.get_sql())

    @classmethod
    def drop_table(cls) -> None:
        """Drop table from the database if it exists

        Raises:
            DropTableNotAllowed: if table dropping is not allowed in the ARORMConfig
            TableDoesNotExist: if table doesn't exist in the database
        """
        if not cls._get_config_value("allow_drop_table"):
            raise DropTableNotAllowed

        if not cls._check_table_existance(cls.__name__.lower()):
            raise TableDoesNotExist(cls.__name__.lower())

        table = cls._get_pypika_table()
        query = pp.Query.drop_table(table)
        cls._execute_query(query.get_sql())

    @classmethod
    def get[T: "BaseModel"](cls: Type[T], key: Key) -> T | None:
        """Get record with specified key

        Returns:
            Record with specified key or None if it doesn't exist
        """
        table = cls._get_pypika_table()
        query: ppq.QueryBuilder = (
            pp.Query.from_(table).where(table.id == key).select("*")
        )
        obj = cls._execute_query(query.get_sql())

        if len(obj) == 0:
            return None

        return cls._parse_model(obj[0])

    def save(self) -> None:
        """Save record to the database (insert if it doesn't exist, update otherwise)"""
        self.save_record(self)

    @classmethod
    def save_record[T: "BaseModel"](cls: Type[T], record: T) -> None:
        """Save record to the database (insert if it doesn't exist, update otherwise)

        Args:
            record: record to save
        """
        table = cls._get_pypika_table()

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

        cls._execute_query(query.get_sql())

    @classmethod
    def all[T: "BaseModel"](cls: Type[T]) -> list[T]:
        """Get all records

        Returns:
            All table records
        """
        return cls.filter()

    @classmethod
    def filter[T: "BaseModel"](cls: Type[T], **kwargs: Any) -> list[T]:
        """Filter records by specified fields

        Returns:
            Filtered records
        """
        cls._validate_query_fields(kwargs.keys())

        table = cls._get_pypika_table()
        query: ppq.QueryBuilder = pp.Query.from_(table)

        for key, val in kwargs.items():
            query = query.where(getattr(table, key) == val)

        query = query.select("*")
        result = cls._execute_query(query.get_sql())

        return [cls._parse_model(obj) for obj in result]

    @classmethod
    def delete(cls, **kwargs: Any) -> None:
        """Delete records with specified fields

        Args:
            **kwargs: fields to filter records for deletion
        """
        cls._validate_query_fields(kwargs.keys())

        table = cls._get_pypika_table()
        query: ppq.QueryBuilder = pp.Query.from_(table)

        for key, val in kwargs.items():
            query = query.where(getattr(table, key) == val)

        query = query.delete()

        cls._execute_query(query.get_sql())
