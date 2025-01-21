#!/bin/bash

FILE1=$1
FILE2=$2
COMM=$(comm -23 <(sort "$FILE1") <(sort "$FILE2") | wc -l)
NR1=$(wc -l < "$FILE1")
NR2=$(wc -l < "$FILE2")

# Calculate percentage using bc
RESULT=$(echo "scale=2; 100 * $COMM / ($NR1 + $NR2)" | bc)
echo "$RESULT %"
