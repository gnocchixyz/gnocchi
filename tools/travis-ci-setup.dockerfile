FROM ubuntu:16.04
ENV GNOCCHI_SRC /home/tester/src
ENV DEBIAN_FRONTEND noninteractive

#NOTE(sileht): really no utf-8 in 2017 !?
ENV LANG en_US.UTF-8
RUN update-locale
RUN locale-gen $LANG

RUN apt-get update -y && apt-get install -qy \
        git \
        wget \
        nodejs \
        nodejs-legacy \
        npm \
        python \
        python3 \
        python-dev \
        python3-dev \
        python-tox \
        redis-server \
        build-essential \
        libffi-dev \
        libpq-dev \
        postgresql \
        mysql-client \
        mysql-server \
        librados-dev \
        ceph \
    && apt-get clean -y

RUN useradd -ms /bin/bash tester
RUN mkdir $GNOCCHI_SRC
RUN chown -R tester: $GNOCCHI_SRC
USER tester
WORKDIR $GNOCCHI_SRC
