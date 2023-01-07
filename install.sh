#!/bin/bash

set -e

echo timedatectl set-timezone Asia/Tokyo
timedatectl set-timezone Asia/Tokyo

logdir=`grep log_dir default.yaml | cut -d":" -f2`
echo mkdir -p $logdir
mkdir -p $logdir

echo apt update
apt update
echo apt -y install nodejs npm python3-pip python3-venv mariadb-server nginx build-essential libssl-dev libffi-dev python-dev libssl-dev
apt -y install nodejs npm python3-pip python3-venv mariadb-server nginx build-essential libssl-dev libffi-dev python-dev libssl-dev


echo 'mysql -uroot < sql/init_database.sql'
mysql -uroot < sql/init_database.sql

echo python3 -m venv .venv
python3 -m venv .venv

echo source .venv/bin/activate
source .venv/bin/activate

echo pip3 install pymysql yfinance pandas_datareader pandas numpy sqlalchemy falcon xlrd path sklearn matplotlib jupyter
pip3 install -r install/pip/requirements.txt

echo pip3 install uwsgi
pip3 install uwsgi

#echo install/uwsgi/create_uwsgi.ini.sh
#bash install/uwsgi/create_uwsgi.ini.sh

echo install/systemd/create_stockanalapi.service.sh
bash install/systemd/create_stockanalapi.service.sh
echo systemctl enable stockanalapi
systemctl enable stockanalapi

echo systemctl reload nginx
systemctl reload nginx

#npm install express
#npm install @amcharts/amcharts5


