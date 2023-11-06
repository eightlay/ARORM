from arorm.model import BaseModel


class TestModel(BaseModel):
    id: int
    name: str
    # TODO: think about it
    # age: int | None = None
    age: int = 0

    class ARORMConfig:
        allow_create_table = False


def main():
    try:
        TestModel.create_table()
    except:
        pass
    try:
        objs = TestModel.all()
    except:
        pass
    try:
        objs = TestModel.filter(id=1)
    except:
        pass
    try:
        TestModel.delete(id=1)
    except:
        pass
    try:
        TestModel.drop_table()
    except:
        pass
    try:
        obj = TestModel.get(key="1")
    except:
        pass
    try:
        TestModel.save_record(record=TestModel(id=1, name="test", age=1))
    except:
        pass


if __name__ == "__main__":
    main()
