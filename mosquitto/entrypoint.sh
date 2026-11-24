#!/bin/sh
set -eu

CERT_DIR="/mosquitto/certs"
AUTH_DIR="/mosquitto/auth"
DATA_DIR="/mosquitto/data"
PASSWD_FILE="${AUTH_DIR}/passwd"

MQTT_USER="${SANMIST_MQTT_USERNAME:-sanmist_svc}"
MQTT_PASS="${SANMIST_MQTT_PASSWORD:-sanmist-broker-2026}"
MQTT_DEVICE_USER="${SANMIST_MQTT_DEVICE_USERNAME:-}"
MQTT_DEVICE_PASS="${SANMIST_MQTT_DEVICE_PASSWORD:-}"
BROKER_SAN="${SANMIST_MQTT_CERT_SAN:-DNS:mosquitto,DNS:localhost,IP:127.0.0.1}"

mkdir -p "${CERT_DIR}" "${AUTH_DIR}" "${DATA_DIR}"

if [ ! -f "${CERT_DIR}/ca.crt" ] || [ ! -f "${CERT_DIR}/server.crt" ] || [ ! -f "${CERT_DIR}/server.key" ]; then
    echo "[MOSQUITTO TLS] Generating local CA and TLS server certificate (SAN=${BROKER_SAN})..."
    EXT_FILE="/tmp/mosquitto_san.ext"
    cat > "${EXT_FILE}" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=${BROKER_SAN}
EOF

    openssl req -x509 -nodes -newkey rsa:2048 -days 825 \
        -keyout "${CERT_DIR}/ca.key" \
        -out "${CERT_DIR}/ca.crt" \
        -subj "/CN=SanMist Local MQTT CA"

    openssl req -nodes -newkey rsa:2048 \
        -keyout "${CERT_DIR}/server.key" \
        -out /tmp/server.csr \
        -subj "/CN=mosquitto"

    openssl x509 -req -days 825 \
        -in /tmp/server.csr \
        -CA "${CERT_DIR}/ca.crt" \
        -CAkey "${CERT_DIR}/ca.key" \
        -CAcreateserial \
        -out "${CERT_DIR}/server.crt" \
        -extfile "${EXT_FILE}"

    rm -f /tmp/server.csr "${EXT_FILE}"
fi

echo "[MOSQUITTO AUTH] Provisioning authenticated broker credentials (allow_anonymous=false)..."
mosquitto_passwd -b -c "${PASSWD_FILE}" "${MQTT_USER}" "${MQTT_PASS}"
if [ -n "${MQTT_DEVICE_USER}" ] && [ -n "${MQTT_DEVICE_PASS}" ]; then
    mosquitto_passwd -b "${PASSWD_FILE}" "${MQTT_DEVICE_USER}" "${MQTT_DEVICE_PASS}"
fi

chmod 644 "${CERT_DIR}/ca.crt" "${CERT_DIR}/server.crt"
chmod 600 "${CERT_DIR}/server.key" "${PASSWD_FILE}"
[ -f "${CERT_DIR}/ca.key" ] && chmod 600 "${CERT_DIR}/ca.key"
chown -R mosquitto:mosquitto "${CERT_DIR}" "${AUTH_DIR}" "${DATA_DIR}"

exec /usr/sbin/mosquitto -c /mosquitto/config/mosquitto.conf
