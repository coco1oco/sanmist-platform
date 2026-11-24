FROM eclipse-mosquitto:2

RUN apk add --no-cache openssl

COPY mosquitto/entrypoint.sh /usr/local/bin/sanmist-mosquitto-entrypoint.sh
RUN sed -i 's/\r$//' /usr/local/bin/sanmist-mosquitto-entrypoint.sh \
    && chmod +x /usr/local/bin/sanmist-mosquitto-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/sanmist-mosquitto-entrypoint.sh"]
