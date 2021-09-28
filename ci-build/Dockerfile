FROM openjdk:11-jdk-slim-buster

COPY ./containerscripts/sbt-install.sh /tmp
COPY ./containerscripts/awstools.sh /tmp
RUN bash /tmp/sbt-install.sh
RUN bash /tmp/awstools.sh