from arorm.model import BaseModel


class TestModel(BaseModel):
    id: int
    name: str
    # TODO: think about it
    # age: int | None = None
    age: int = 0


def main():
    TestModel.create_table()
    TestModel.save_record(record=TestModel(id=1, name="test", age=1))
    objs = TestModel.filter(id=1)
    print(objs)
    obj = TestModel.get(key="1")
    print(obj)
    TestModel.delete(id=1)
    objs = TestModel.all()
    print(objs)
    TestModel.drop_table()


if __name__ == "__main__":
    main()
