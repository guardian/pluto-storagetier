FROM openjdk:11-jdk-slim-buster

COPY ./containerscripts/sbt-deb-key.txt /tmp
COPY ./containerscripts/sbt-install.sh /tmp
COPY ./containerscripts/awstools.sh /tmp
RUN bash /tmp/sbt-install.sh
RUN apt-get update && apt-get -y install awscli && apt-get -y autoremove && rm -rf /var/cache/apt

COPY ./containerscripts/sbt-repositories /root/.sbt