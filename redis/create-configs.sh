#!/bin/bash

set -e
set +x

self_dir="$(realpath $(dirname "$0"))"

source ${self_dir}/.env

cat > "${self_dir}/redis.conf" <<EOF
bind 0.0.0.0
requirepass $REDIS_PASSWORD
appendonly yes
appendfsync always
EOF

cat > "${self_dir}/users.acl" <<EOF
user default on nopass ~* +@all
user $REDIS_USER on >$REDIS_USER_PASSWORD ~* &* +@all
EOF


