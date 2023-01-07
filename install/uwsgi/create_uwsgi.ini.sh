currdir=`pwd`
cat << EOF > uwsgi.ini
[uwsgi]
master = 1
vacuum = true
socket = 127.0.0.1:9001
enable-threads = true
thunder-lock = true
threads = 2
processes = 2
virtualenv = $currdir/.venv
wsgi-file = $currdir/pyapi/uwsgi.py
chdir = $currdir
uid = root
gid = root
EOF

