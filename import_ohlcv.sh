#!/bin/bash

sudo service mysql start

python3 lib/data_import.py
