FROM ubuntu:20.04
ENV GNOCCHI_WORKSPACE /github/workspace
ENV DEBIAN_FRONTEND noninteractive

#NOTE(tobias-urdin): need gnupg for apt-key
RUN apt-get update -y && apt-get install -qy gnupg software-properties-common
RUN add-apt-repository -y ppa:deadsnakes/ppa
RUN apt-get update -y && apt-get install -qy \
        locales \
        git \
        wget \
        curl \
        nodejs \
        npm \
        python3 \
        python3-dev \
        python3-pip \
        python3.6 \
        python3.6-dev \
# Needed for uwsgi core routing support
        build-essential \
        libffi-dev \
        libpq-dev \
        postgresql \
        memcached \
        mysql-client \
        mysql-server \
# For Ceph
        librados-dev \
        liberasurecode-dev \
        python3-rados \
        ceph \
# For prometheus
        libsnappy-dev \
        libprotobuf-dev \
# For redis
        redis-server \
        && rm -rf /var/lib/apt/lists/*

#NOTE(sileht): really no utf-8 in 2017 !?
ENV LANG en_US.UTF-8
RUN update-locale
RUN locale-gen $LANG

#NOTE(sileht): Upgrade python dev tools
RUN python3 -m pip install -U pip tox virtualenv
RUN python3.6 -m pip install -U pip tox virtualenv

RUN npm install s3rver@3.7.0 --global

RUN groupadd --gid 1001 tester
RUN useradd --uid 1001 --gid 1001 --home-dir $GNOCCHI_WORKSPACE --shell /bin/bash tester
USER tester

COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
