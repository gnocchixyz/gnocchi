FROM ubuntu:20.04
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -y && apt-get install -qy \
        python3 \
        python3-dev \
        python3-pip \
        && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install -U pip tox virtualenv

WORKDIR /gnocchi
