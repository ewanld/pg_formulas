#! /bin/bash

sed -n -E 's/^.*--\s*(.*)$/, \1/p' code.txt
