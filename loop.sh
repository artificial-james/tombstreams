#!/bin/bash

code=0

while [ $code -eq 0 ]
do
  go test ./... -count=1 -timeout=2s
  code=$?
done
