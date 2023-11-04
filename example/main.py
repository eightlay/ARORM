from arorm.dao import BaseDAO
from arorm.model import BaseModel


class TestModel(BaseModel):
    id: int
    name: str
    # TODO: think about it
    # age: int | None = None
    age: int = 0


class TestDAO(BaseDAO[TestModel]):
    model = TestModel


def main():
    try:
        TestDAO.create_table()
    except:
        pass
    try:
        TestDAO.all()
    except:
        pass
    try:
        TestDAO.filter(id=1)
    except:
        pass
    try:
        TestDAO.delete(id=1)
    except:
        pass
    try:
        TestDAO.drop_table()
    except:
        pass
    try:
        TestDAO.get(key="1")
    except:
        pass
    try:
        TestDAO.save(record=TestModel(id=1, name="test", age=1))
    except:
        pass


if __name__ == "__main__":
    main()
