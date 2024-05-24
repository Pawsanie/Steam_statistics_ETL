# System initialization:
FROM vault:1.13.3

# Set Environment:
RUN \
  root_token_id=$(head -c 16 /dev/urandom | base64 | tr -d '+/=') \
  mkdir /vault_data && \
  touch /vault/config/vault.hcl && \
\
  echo -e '{\n\
  "storage": {\n\
    "file": {\n\
      "path": "/vault_data/file"\n\
    }\n\
  },\n\
  "listener": [\n\
    {\n\
      "tcp": {\n\
        "address": "0.0.0.0:8200",\n\
        "tls_disable": true\n\
      }\n\
    }\n\
  ],\n\
  "default_lease_ttl": "168h",\n\
  "max_lease_ttl": "720h",\n\
  "ui": true\n\
  "log": {\n\
    "format": "json",\n\
    "level": "debug",\n\
    "output": "/vault/logs/vault.log"\n\
  }\n\
}' > /vault/config/vault.hcl

ENV \
  VAULT_DEV_ROOT_TOKEN_ID=$root_token_id \
  VAULT_ADDR="http://localhost:8200" \
  VAULT_DEV_LISTEN_ADDRESS="0.0.0.0:8200"

# Enable IPC_LOCK and Bash:
RUN \
  set -eux && apk --no-cache add su-exec bash
  
# Vault initialization:
RUN \
  mkdir /vault/init && \
  touch /vault/init/init.sh && \
  touch /vault_data/vault_keys && \
  echo -e "#!/bin/bash\n\
init_output=\$(vault operator init -key-shares=1 -key-threshold=1 2>&1)\n\
if ! \$(echo -e \"\$init_output\" | grep -q \"Vault is already initialized\"); then\n\
    unsealing_key=\$(echo -e \"\$init_output\" | grep \"Unseal Key 1:\" | awk '/Unseal Key 1:/ {print \$4}')\n\
    root_token=\$(echo -e \"\$init_output\" | grep \"Initial Root Token:\" | awk '/Initial Root Token:/ {print \$4}')\n\
    echo \"\$unsealing_key\" > /vault_data/vault_keys\n\
    echo \"\$root_token\" >> /vault_data/vault_keys\n\
    vault operator unseal \"\$unsealing_key\"\n\
\
    vault login $root_token\n\
    password_dba=$(head -c 16 /dev/urandom | base64 | tr -d '+/=')\n\
    password_Luigi=$(head -c 16 /dev/urandom | base64 | tr -d '+/=')\n\
    vault kv put secret/steam_statistics/dba value="$password_dba"\n\
    vault kv put secret/steam_statistics/luigi value="$password_Luigi"\n\
    unset VAULT_TOKEN\n\
else\n\
    unsealing_key=\$(sed -n \"1p\" /vault_data/vault_keys)\n\
    vault operator unseal \$unsealing_key\n\
fi\
" > /vault/init/init.sh  && \
  chmod +x /vault/init/init.sh
  
# ENTRYPOINT initialization:
RUN \
  touch /vault/init/ENTRYPOINT.sh && \
  echo -e "#!/bin/bash\n\
\
vault server -config=/vault/config/vault.hcl &\n\
vault_pid=\$! &\n\
sleep 5 &&\n\
/vault/init/init.sh \n\
wait \$vault_pid\
" > /vault/init/ENTRYPOINT.sh  && \
     chmod +x /vault/init/ENTRYPOINT.sh

# Doker initialization:
ENTRYPOINT ["/vault/init/ENTRYPOINT.sh"]

LABEL server="Vault" destination="STEAM"