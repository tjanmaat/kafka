#!/bin/bash
# build container
docker build . -f Dockerfile_producer -t producer:latest
# serve containers
docker-compose up -d

# do not close terminal right away
read

docker-compose down