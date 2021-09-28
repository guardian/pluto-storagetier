#!/usr/bin/env bash

apt-get update
apt-get -y install python3
curl "https://bootstrap.pypa.io/get-pip.py" -o "/tmp/get-pip.py"
chmod a+x /tmp/get-pip.py
python3 /tmp/get-pip.py
rm -f /tmp/get-pip.py

pip3 install awscli

apt-get autoremove
rm -rf /var/cache/apt