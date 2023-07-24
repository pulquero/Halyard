#!/bin/bash

curl \
  --data-urlencode "update=INSERT { ?s ?p ?o } WHERE { ?s ?p ?o }" \
  --data-urlencode "map-reduce=true" \
  $ENDPOINT > ${1}
