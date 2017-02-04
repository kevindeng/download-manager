#!/bin/bash

set -o pipefail

curl -m 10 -v -H "range: bytes=0-0" "$1" 2>&1 | grep Content-Range | cut -d '/' -f 2
