#!/bin/bash

# sleep 500
while :; do
    gtimeout 60s python3.12 scraping.py --num-processes 50
    sleep 120
done
