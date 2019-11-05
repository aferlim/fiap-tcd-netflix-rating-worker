FROM node:12-alpine
MAINTAINER Andre Lima <aferlim@gmail.com>

# This is the release of Consul to pull in.
ENV CONSUL_VERSION=1.6.1

RUN apk update && \
    apk add \
    bash curl nano net-tools zip unzip \
    jq iputils

# Setup Consul and Goreman
RUN mkdir -p var/data /etc/consul.d

ADD https://releases.hashicorp.com/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_linux_amd64.zip /tmp/consul.zip
RUN cd /bin && unzip /tmp/consul.zip && chmod +x /bin/consul && rm /tmp/consul.zip


ADD ./consul/config /etc/consul.d
#ADD Procfile /root/Procfile

# Setting workdir
ADD ./consul/consul.sh /opt
ADD run.sh /opt


WORKDIR /home/node/rating

ADD package*.json ./
RUN npm i

ADD ./src ./src/

EXPOSE 8080

ENTRYPOINT [ "sh", "/opt/run.sh" ]