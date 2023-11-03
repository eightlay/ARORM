import pydantic
import pypika as pp
from typing import Any

from arorm.key import Key, generate_tsid


class BaseModel(pydantic.BaseModel):
    class Config:
        extra = "forbid"

    id: Key = pydantic.Field(default_factory=generate_tsid)

    @classmethod
    def get_pypika_table(cls) -> pp.Table:
        return pp.Table(cls.__name__.lower())

    @staticmethod
    def translate_pydantic_to_pypika(
        pydantic_schema: dict[str, Any]
    ) -> list[pp.Column]:
        result: list[pp.Column] = []

        for col_name in pydantic_schema["properties"]:
            # TODO: handle anyOf
            col_type = pydantic_schema["properties"][col_name]["type"]
            nullable = col_name not in pydantic_schema["required"]

            # TODO: support more column settings
            result.append(pp.Column(col_name, col_type, nullable=nullable))

        return result
