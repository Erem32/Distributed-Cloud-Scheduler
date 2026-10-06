"""Optional transport settings, independent of local/domain configuration."""

import ipaddress
import re
from urllib.parse import urlsplit

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PubSubSettings(BaseSettings):
    """Read Pub/Sub settings only when a transport runtime is constructed."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
    )

    pubsub_topic: str | None = None
    pubsub_subscription: str | None = None
    pubsub_emulator_host: str | None = None
    push_request_timeout_seconds: float = Field(default=45, gt=0, le=60, allow_inf_nan=False)

    @field_validator("pubsub_topic")
    @classmethod
    def validate_topic(cls, value: str | None) -> str | None:
        return cls._validate_resource(value, "topics", "PUBSUB_TOPIC")

    @field_validator("pubsub_subscription")
    @classmethod
    def validate_subscription(cls, value: str | None) -> str | None:
        return cls._validate_resource(value, "subscriptions", "PUBSUB_SUBSCRIPTION")

    @staticmethod
    def _validate_resource(value: str | None, collection: str, setting: str) -> str | None:
        if value is None:
            return None
        pattern = (
            rf"projects/[A-Za-z0-9][A-Za-z0-9._:-]*/{collection}/"
            r"[A-Za-z][A-Za-z0-9\-_.~+%]{2,254}"
        )
        if re.fullmatch(pattern, value) is None or value.rsplit("/", 1)[1].startswith("goog"):
            raise ValueError(f"{setting} must be a full Pub/Sub {collection} resource name")
        return value

    @field_validator("pubsub_emulator_host")
    @classmethod
    def validate_emulator_host(cls, value: str | None) -> str | None:
        """Accept explicit host:port targets, including bracketed IPv6, without URLs."""
        if value is None:
            return None
        valid = False
        try:
            target = urlsplit(f"http://{value}")
            host, port = target.hostname, target.port
            valid = bool(
                host
                and port is not None
                and 1 <= port <= 65535
                and target.username is None
                and target.password is None
                and target.netloc == value
                and not target.path
                and not target.query
                and not target.fragment
                and not any(character.isspace() for character in value)
            )
            if host and ":" in host:
                ipaddress.IPv6Address(host)
            elif host and re.fullmatch(r"[A-Za-z0-9._-]+", host) is None:
                valid = False
        except ValueError:
            valid = False
        if not valid:
            raise ValueError("PUBSUB_EMULATOR_HOST must be a host:port target without credentials")
        return value
