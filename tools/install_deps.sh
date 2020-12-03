#!/bin/bash -ex

#NOTE(tobias-urdin): need gnupg for apt-key
sudo apt-get update -y
sudo apt-get install -qy gnupg
echo 'deb http://ppa.launchpad.net/deadsnakes/ppa/ubuntu bionic main' | sudo tee /etc/apt/sources.list.d/deadsnakes.list
sudo apt-key adv --recv-keys --keyserver keyserver.ubuntu.com F23C5A6CF475977595C89F51BA6932366A755776
sudo apt-get update -y
sudo apt-get install -qy \
        locales \
        git \
        wget \
        curl \
        nodejs \
        npm \
        python3 \
        python3-dev \
        python3-pip \
        build-essential \
        libffi-dev \
        libpq-dev \
        postgresql \
        memcached \
        mysql-client \
        mysql-server \
        librados-dev \
        liberasurecode-dev \
        python3-rados \
        ceph \
        libsnappy-dev \
        libprotobuf-dev \
        redis-server

sudo rm -rf /var/lib/apt/lists/*

export LANG=en_US.UTF-8
sudo update-locale
sudo locale-gen $LANG

#NOTE(sileht): Upgrade python dev tools
sudo python3.6 -m pip install -U pip tox virtualenv

sudo npm install s3rver@1.0.3 --global
