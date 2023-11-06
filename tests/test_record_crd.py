import pytest
from typing import Type

from arorm import BaseModel
from arorm.exceptions import (
    TableDoesNotExist,
    DropTableNotAllowed,
    CreateTableNotAllowed,
)


class TestModel(BaseModel):
    name: str
    age: int = 0


@pytest.fixture
def test_model():
    TestModel.create_table()

    yield TestModel

    TestModel.drop_table()


def test_insert(test_model: Type[TestModel]):
    try:
        test_model(name="Chandler", age=30).save()
    except Exception as e:
        raise Exception("Insert failed") from e


def test_get(test_model: Type[TestModel]):
    obj = test_model(name="Chandler", age=30)
    obj.save()

    obj = test_model.get(obj.id)

    assert obj is not None, "Get failed (object not found)"


def test_update(test_model: Type[TestModel]):
    obj = test_model(name="Chandler", age=30)
    obj.save()
    obj.age = 31
    obj.save()

    obj = test_model.get(obj.id)

    if obj is None:
        raise Exception("Update failed (object not found)")

    assert obj.age == 31, "Update failed (age not updated)"


def test_delete(test_model: Type[TestModel]):
    obj = test_model(name="Chandler", age=30)
    obj.save()
    obj.delete(id=obj.id)

    obj = test_model.get(obj.id)

    assert obj is None, "Delete failed (object not deleted)"


def test_select_all(test_model: Type[TestModel]):
    test_model(name="Chandler", age=30).save()
    test_model(name="Joey", age=30).save()

    obj = test_model.all()

    assert len(obj) == 2, "Select all failed (wrong number of objects)"


def test_select_filtered(test_model: Type[TestModel]):
    test_model(name="Chandler", age=30).save()
    test_model(name="Joey", age=31).save()

    obj = test_model.filter(age=30)

    assert len(obj) == 1, "Select filtered failed (wrong number of objects)"
