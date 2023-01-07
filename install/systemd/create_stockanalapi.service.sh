cat << EOF > /etc/systemd/system/stockanalapi.service
[Unit]
Description=wsgi for Stockanalyzer api 

# Requirements
Requires=network.target

# Dependency ordering
After=network.target

[Service]
TimeoutStartSec=0
RestartSec=10
Restart=always

# path to app
WorkingDirectory=`pwd`
# the user that you want to run app by
User=root

KillSignal=SIGQUIT
Type=notify
NotifyAccess=all

# Main process
ExecStart=$(pwd)/.venv/bin/uwsgi -c $(pwd)/uwsgi.ini
#ExecStart=/usr/bin/python3 $(pwd)/pyapi/app.py

[Install]
WantedBy=multi-user.target
EOF
