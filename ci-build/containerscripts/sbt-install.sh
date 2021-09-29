#!/bin/bash -e

apt-get -y update
apt-get -y install curl gnupg

echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add
apt-get -y update
apt-get -y install sbt git libxml2-utils
apt-get -y autoremove && apt-get -y clean && rm -rf /var/cache/apt

echo exit | sbt

curl -L https://download.docker.com/linux/static/stable/x86_64/docker-18.06.1-ce.tgz > /tmp/docker-18.06.1-ce.tgz
tar xvzf /tmp/docker-18.06.1-ce.tgz
mv docker/docker /usr/bin
rm -rf docker
