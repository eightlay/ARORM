from arorm import BaseModel
from arorm.exceptions import (
    TableDoesNotExist,
    DropTableNotAllowed,
    CreateTableNotAllowed,
)


class DefaultTestModel(BaseModel):
    name: str
    age: int = 0


class NothingAllowedTestModel(BaseModel):
    class DefaultARORMConfig:
        allow_create_table = False
        allow_drop_table = False

    name: str
    age: int = 0


def test_table_creation():
    DefaultTestModel.create_table()


def test_table_recreation():
    try:
        DefaultTestModel.create_table()
        DefaultTestModel.create_table()
    except Exception as e:
        raise Exception("Table recreation failed") from e


def test_table_drop():
    try:
        DefaultTestModel.create_table()
    except:
        pass

    try:
        DefaultTestModel.drop_table()
    except Exception as e:
        raise Exception("Table drop failed") from e


def test_non_existing_table_drop():
    try:
        DefaultTestModel.drop_table()
    except:
        pass

    try:
        DefaultTestModel.drop_table()
    except TableDoesNotExist:
        return
    except Exception as e:
        raise Exception("Non existing table drop failed") from e


def test_create_table_restricted():
    try:
        NothingAllowedTestModel.create_table()
    except CreateTableNotAllowed:
        return
    except Exception as e:
        raise Exception("Table creation restriction failed") from e


def test_drop_table_restricted():
    try:
        NothingAllowedTestModel.drop_table()
    except DropTableNotAllowed:
        return
    except Exception as e:
        raise Exception("Table drop restriction failed") from e
