import pydantic
from typing import Any

from arorm.key import Key


class BaseDAO[Model: pydantic.BaseModel]:
    def create_table(self) -> None:
        raise NotImplemented

    def drop_table(self) -> None:
        raise NotImplemented

    def save(self, record: Model) -> None:
        raise NotImplemented

    def get(self, key: Key) -> Model:
        raise NotImplemented

    def filter(self, **kwargs: Any) -> list[Model]:
        raise NotImplemented

    def delete(self) -> bool:
        raise NotImplemented

    def all(self) -> list[Model]:
        raise NotImplemented
