#!/usr/bin/env bash

docker network ls | grep storagetier
if [ "$?" != "0" ]; then
    docker network create storagetier
fi

docker ps | grep storagetier-databas
if [ "$?" == "0" ]; then
    docker rm storagetier-databas
fi

docker run --rm -p 5432:5432 --env-file postgres.env --network storagetier --name storagetier-database -v ${PWD}/docker-init:/docker-entrypoint-initdb.d postgres:13-alpine
