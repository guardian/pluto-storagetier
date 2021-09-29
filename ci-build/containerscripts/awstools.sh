#!/usr/bin/env bash

apt-get update
apt-get -y install python3 python3-pip

pip3 install awscli

apt-get autoremove
rm -rf /var/cache/apt