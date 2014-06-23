========
 Gnocchi
========

REST API to store metrics in object storage.

`Gnocchi <https://wiki.openstack.org/wiki/Gnocchi>`_ uses Pandas to analyze
metrics, with Swift as the canonical storage driver (other pluggable drivers
are expected to follow).

Installation Instructions
=========================
Install Swift, either by enabling the required services in a full devstack
environment, or as a stand-alone installation.

To enable Swift in Devstack, edit your localrc file to include::

    enable_service s-proxy s-account s-container s-object

and run `./stack.sh`.

For directions on installing Swift all-in-one, see
`these instructions <https://docs.openstack.org/developer/swift/development_saio.html>`_.

Clone the gnocchi git repo (if you have a full Devstack environment, the
usual directory in which to install gnocchi would be /opt/stack/)::

    cd /opt/stack && git clone https://github.com/stackforge/gnocchi.git

You may need to install the following libraries, depending on your system;
on Ubuntu the command would be::

    sudo apt-get install build-essential libpq-dev libx11-dev libasound2-dev

for Fedora users, type::

    sudo yum install gcc-c++ libpq-devel libx11-devel alsa-lib-devel

Run the installer::

    cd gnocchi
    sudo pip install -r requirements.txt
    sudo pip install -r test-requirements.txt
    sudo python setup.py install


Configuring Gnocchi
===================

If it doesn't exist, create a gnocchi.conf file in /etc/gnocchi/::

    mkdir -p /etc/gnocchi
    cd /etc/gnocchi && touch gnocchi.conf

Edit `/etc/gnocchi/gnocchi.conf`. Shown below is a sample configuration file::

    [api]
    port = 8041
    host = 0.0.0.0

    [storage]
    swift_auth_version = 1
    swift_authurl = http://localhost:8080/auth/v1.0
    swift_user = admin:admin
    swift_key = admin
    swift_coordination_driver = memcached

    [indexer]
    driver = sqlalchemy

    [database]
    connection = mysql://username:password@host/gnocchi

To use postgresql instead, set the database connection string accordingly::

    connection = postgres://username:pasword@host/gnocchi

Create a database. For mysql, from the command line type::

    mysql -u root -pPASSWORD -e "create database gnocchi;"

For postgresql::

    createdb -U USERNAME -T template0 gnocchi

Initialize the database by running::

    gnocchi-dbsync

Sending Requests to the API
===========================

Run the Gnocchi API service::

    gnocchi-api

You can now send requests to the API. Here's an example that creates an
entity with an archive that stores one point every second for an hour
(shown both with the curl command and using a Python script)::

    curl -i http://0.0.0.0:8041/v1/entity -X POST \
      -H "Content-Type: application/json" -H "Accept: application/json" \
      -d '{"archives": [[1, 3600]]}'

Or::

    import requests
    import json

    r = requests.post('http://0.0.0.0:8041/v1/entity', data=json.dumps({"archives": [[1, 3600]]}))
    print r.status_code
    print r.text
