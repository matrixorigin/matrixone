#!/bin/bash

data=$(</dev/stdin)
echo "[\"This is answer from LLM. Input Data: $data. Index Table: $1.\"]"
