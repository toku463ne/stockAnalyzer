#!/bin/bash

sudo service mysql start

python3 pyapi/app.py
