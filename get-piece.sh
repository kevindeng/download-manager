#!/bin/bash

set -o pipefail

curl -s -m 120 -H "range: bytes=$2-$3" "$1" | dd bs="$4" of="$5" conv=notrunc seek="$6" >/dev/null 2>/dev/null
