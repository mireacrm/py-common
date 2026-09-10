"""Настройки, общие для всех сервисов.

Сервис наследует `ServiceSettings`, задаёт свой префикс переменных окружения
и добавляет то, что есть только у него: адреса соседей по gRPC, ключи очередей.
Общими остаются поля, которые есть у каждого: своя база, свои порты, брокер
и адрес коллектора трасс.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class ServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Имя сервиса едет в метрики, спаны и заголовок издателя событий,
    # поэтому переопределить его обязан каждый.
    service_name: str
    postgres_dsn: str
    http_port: int
    grpc_port: int
    amqp_url: str = "amqp://guest:guest@localhost:5672/"
    debug: bool = False
    # Пустой адрес выключает экспорт трасс: нужен для тестов
    # и запуска без инфраструктуры.
    otlp_endpoint: str = ""
