from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    database_url: str = Field(..., env="DATABASE_URL")
    shop_name: str = Field(..., env="SHOP_NAME")
    shop_url: str = Field(..., env="SHOP_URL")
    shop_token: str = Field(..., env="SHOP_TOKEN")
    shopify_api_version: str = Field(..., env="SHOPIFY_API_VERSION")

    class Config:
        env_file = ".env"
        extra = "forbid"a


settings = Settings()
