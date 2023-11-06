from pydantic_settings import BaseSettings


class Config(BaseSettings):
    PG_HOST: str
    PG_PORT: int
    PG_USER: str
    PG_PASS: str
    PG_DB: str

    class Config:
        env_file = ".env"


config = Config()  # type: ignore
