from dataclasses import dataclass
import os
import ssl
from typing import Any, Mapping, Optional

from fastapi import FastAPI, Request

DEFAULT_CORS_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
]

_FALSY_VALUES = {"0", "false", "no", "off"}
_TRUTHY_VALUES = {"1", "true", "yes", "on"}


def _parse_bool(raw: Optional[str], default: bool) -> bool:
    if raw is None or str(raw).strip() == "":
        return default
    normalized = str(raw).strip().lower()
    if normalized in _TRUTHY_VALUES:
        return True
    if normalized in _FALSY_VALUES:
        return False
    return default


@dataclass(frozen=True)
class MqttSecurityConfig:
    host: str
    port: int
    use_tls: bool
    ca_cert_path: Optional[str]
    tls_insecure: bool
    username: str
    password: str
    require_payload_auth: bool


def load_mqtt_security_config(
    env: Optional[Mapping[str, str]] = None,
) -> MqttSecurityConfig:
    source = os.environ if env is None else env
    host = str(source.get("SANMIST_MQTT_BROKER") or "broker.emqx.io").strip()
    use_tls = _parse_bool(source.get("SANMIST_MQTT_TLS"), default=True)
    default_port = "8883" if use_tls else "1883"
    port = int(str(source.get("SANMIST_MQTT_PORT") or default_port).strip())
    ca_cert_raw = str(source.get("SANMIST_MQTT_CA_CERT") or "").strip()
    ca_cert_path = ca_cert_raw or None
    tls_insecure = _parse_bool(source.get("SANMIST_MQTT_TLS_INSECURE"), default=False)
    username = str(source.get("SANMIST_MQTT_USERNAME") or "").strip()
    password = str(source.get("SANMIST_MQTT_PASSWORD") or "")
    require_payload_auth = _parse_bool(
        source.get("SANMIST_MQTT_REQUIRE_AUTH"), default=True
    )
    return MqttSecurityConfig(
        host=host,
        port=port,
        use_tls=use_tls,
        ca_cert_path=ca_cert_path,
        tls_insecure=tls_insecure,
        username=username,
        password=password,
        require_payload_auth=require_payload_auth,
    )


def build_mqtt_ssl_context(config: MqttSecurityConfig) -> ssl.SSLContext:
    if config.ca_cert_path:
        if not os.path.isfile(config.ca_cert_path):
            raise FileNotFoundError(
                f"Configured MQTT CA certificate not found: {config.ca_cert_path}"
            )
        ctx = ssl.create_default_context(cafile=config.ca_cert_path)
    else:
        ctx = ssl.create_default_context()

    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    if config.tls_insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    else:
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def configure_mqtt_client_security(
    client: Any, config: MqttSecurityConfig
) -> dict[str, bool]:
    credentials_configured = bool(config.username)
    if credentials_configured:
        client.username_pw_set(config.username, config.password)

    if hasattr(client, "reconnect_delay_set"):
        client.reconnect_delay_set(min_delay=1, max_delay=30)

    if config.use_tls:
        ssl_ctx = build_mqtt_ssl_context(config)
        client.tls_set_context(ssl_ctx)

    return {
        "tls_enabled": config.use_tls,
        "credentials_configured": credentials_configured,
        "ca_cert_used": bool(config.ca_cert_path),
    }


def parse_cors_origins(raw_value: Optional[str]) -> list[str]:
    if not raw_value or not str(raw_value).strip():
        return list(DEFAULT_CORS_ORIGINS)

    seen: set[str] = set()
    origins: list[str] = []
    for part in str(raw_value).split(","):
        cleaned = part.strip().rstrip("/")
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            origins.append(cleaned)
    return origins or list(DEFAULT_CORS_ORIGINS)


def register_security_headers_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _apply_security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault(
            "Referrer-Policy", "strict-origin-when-cross-origin"
        )
        response.headers.setdefault(
            "Permissions-Policy", "camera=(), microphone=(), geolocation=()"
        )
        response.headers.setdefault(
            "Cross-Origin-Opener-Policy", "same-origin-allow-popups"
        )
        forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",")[0].strip().lower()
        if request.url.scheme == "https" or forwarded_proto == "https":
            response.headers.setdefault(
                "Strict-Transport-Security", "max-age=31536000; includeSubDomains"
            )
        return response
