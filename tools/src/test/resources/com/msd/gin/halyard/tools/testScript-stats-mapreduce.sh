#!/bin/bash

curl \
  -H "Content-Type: application/x-halyard-stats"
  --data-urlencode "map-reduce=true" \
  $ENDPOINT > ${1}
