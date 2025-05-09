# syntax = docker/dockerfile:1.2
FROM node:22-bookworm-slim

ARG ENV

RUN apt-get update -y && apt-get install curl -y

WORKDIR /usr/src/avsrouter

VOLUME /usr/src/avsrouter/challenges /usr/src/avsrouter/certs 

COPY package*.json ./

COPY . .

RUN if [ "$ENV" = "debug" ] ; then npm install ; else  npm ci --only=production; fi

ENV AVSROUTERHOST = '0.0.0.0'
ENV AVSROUTERPORT = 443

EXPOSE 80
EXPOSE 443/udp
EXPOSE 443

ENV AVSROUTERACMEHTTP1DIR = '/acmehttp'


CMD [ "node", "src/server.js" ]
