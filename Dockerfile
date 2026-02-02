# Minimální Docker image pro postgres-explorer --stateless
# Založeno na openSUSE Tumbleweed (nejnovější OSS verze)
FROM opensuse/tumbleweed:latest

# Instalace potřebných utilit a PostgreSQL/SQLite nástrojů
RUN zypper --non-interactive refresh && \
    zypper --non-interactive install --no-recommends \
    # Síťové a diagnostické nástroje
    curl \
    netcat-openbsd \
    iputils \
    iproute2 \
    # Textové editory a utility
    vim \
    tmux \
    grep \
    gawk \
    sed \
    # PostgreSQL 18.x klient nástroje
    postgresql18 \
    postgresql18-contrib \
    # SQLite nejnovější
    sqlite3 \
    && zypper clean --all

# Vytvoření uživatele 'app' s UID/GID 1001
RUN groupadd -g 1001 app && \
    useradd -u 1001 -g 1001 -d /app -m -s /bin/bash app

# Nastavení pracovního adresáře a OpenShift kompatibility
WORKDIR /app

# Kopírování staticky linkované binárky s právy pro všechny uživatele (OpenShift compatibility)
COPY target/x86_64-unknown-linux-musl/release/postgres-explorer /app/postgres-explorer

# Kopírování statických souborů (JS, CSS, atd.)
COPY static /app/static

# OpenShift compatibility: /app musí být zapisovatelný pro libovolné UID
RUN chmod -R g=u /app && \
    chmod 755 /app/postgres-explorer && \
    chgrp -R 0 /app && \
    chmod -R g=u /app /etc/passwd

# Přepnutí na neprivilegovaného uživatele (OpenShift stejně použije náhodné UID)
USER 1001

# Výchozí port (pokud postgres-explorer používá nějaký web port)
EXPOSE 8080

# Healthcheck endpoint
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/healthz || exit 1

ENV RUST_LOG=debug

# Výchozí příkaz - stateless režim
ENTRYPOINT ["/app/postgres-explorer"]
CMD ["--stateless"]
