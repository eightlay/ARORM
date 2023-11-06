class CreateTableNotAllowed(Exception):
    def __init__(self) -> None:
        super().__init__("Create table is not allowed")


class DropTableNotAllowed(Exception):
    def __init__(self) -> None:
        super().__init__("Drop table is not allowed")


class ExtraFieldNotAllowed(Exception):
    def __init__(self, field_name: str) -> None:
        super().__init__(f"Extra field '{field_name}' is not allowed in the query")
