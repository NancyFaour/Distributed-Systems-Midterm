#!/bin/sh
set -eu
export NGINX_RESOLVER="$(awk '/^nameserver/ {print $2; exit}' /etc/resolv.conf)"
export GATEWAY_1_ADDRESS="${GATEWAY_1_ADDRESS:-gateway-1:8000}"
export GATEWAY_2_ADDRESS="${GATEWAY_2_ADDRESS:-gateway-2:8000}"
exec /docker-entrypoint.sh nginx -g 'daemon off;'
