# System initialization:
FROM postgres:16.1

# Debian initialization:
USER root
RUN \
    apt update && \
    apt-get install -y curl && \
    apt-get install -y lsb-base && \
    apt-get install -y jq && \
    apt-get install -y wget
RUN \
    wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg && \
    gpg --no-default-keyring --keyring /usr/share/keyrings/hashicorp-archive-keyring.gpg --fingerprint && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(grep VERSION_CODENAME /etc/os-release | cut -d= -f2) main" \
    | tee /etc/apt/sources.list.d/hashicorp.list && \
    apt update
RUN \
    apt-get install -y vault && \
    apt-get clean

ENV \
  POSTGRES_USER="dba" \
  POSTGRES_PASSWORD="$(vault read -address="$vault_address" -format=json secret/steam_statistics/dba | jq -r '.data_base.password')"

# PostgreSQL initialization:
USER root
RUN \
    vault_address="http://$(getent hosts vault | awk '{ print $1 }'):8200" \
    password_dba=$(vault read -address="$vault_address" -format=json secret/steam_statistics/dba | jq -r '.data_base.password') \
    password_Luigi=$(vault read -address="$vault_address" -format=json secret/steam_statistics/luigi | jq -r '.data_base.password') \
    echo "#!/bin/bash\n\

set -e\n\

psql -v ON_ERROR_STOP=1 -U "dba" <<-EOSQL\n\

CREATE USER luigi WITH PASSWORD '$password_Luigi';\n\

CREATE DATABASE steam_statistics;\n\

GRANT ALL PRIVILEGES ON DATABASE steam_statistics TO dba;\n\

GRANT CONNECT ON DATABASE steam_statistics TO luigi;\n\
GRANT USAGE ON SCHEMA public TO luigi;\n\
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA public TO luigi;\n\
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO luigi;\n\

EOSQL" > /docker-entrypoint-initdb.d/init-user-db.sh

# Doker initialization:
LABEL server="PostgreSQL" destination="STEAM"