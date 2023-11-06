from arorm.model import BaseModel


class TestModel(BaseModel):
    name: str
    age: int = 0

    class DefaultARORMConfig:
        allow_extra_query_fields = False


def main():
    # Create table
    TestModel.create_table()

    # Add records
    TestModel.save_record(record=TestModel(name="Joey"))
    TestModel.save_record(record=TestModel(name="Rachel", age=30))
    chan_record = TestModel(name="Chandler", age=30)
    chan_record.save()

    # Get all records
    objs = TestModel.all()
    print("All objects\n", objs)

    # Get records with specified parameters
    objs = TestModel.filter(age=30)
    print("\n\nObjects with age 30\n", objs)

    # Get record with specified key
    obj = TestModel.get(key=chan_record.id)
    print("\n\nObject with specified key\n", obj)

    # Update record
    chan_record.age = 31
    chan_record.save()
    obj = TestModel.get(key=chan_record.id)
    print("\n\nObject after update\n", obj)

    # Delete record with specified key
    TestModel.delete(id=chan_record.id)
    objs = TestModel.all()
    print("\n\nAll objects after 'chan_record' deletion\n", objs)

    # Drop table
    TestModel.drop_table()


if __name__ == "__main__":
    main()
