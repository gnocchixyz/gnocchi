==============
 Installation
==============

.. _installation:

Installation
============

To install Gnocchi using `pip`, just type::

  pip install gnocchi

Depending on the drivers and features you want to use (see :doc:`architecture`
for which driver to pick), you need to install extra variants using, for
example::

  pip install gnocchi[postgresql,ceph,keystone]

This would install PostgreSQL support for the indexer, Ceph support for
storage, and Keystone support for authentication and authorization.

The list of variants available is:

* keystone – provides Keystone authentication support
* mysql - provides MySQL indexer support
* postgresql – provides PostgreSQL indexer support
* swift – provides OpenStack Swift storage support
* s3 – provides Amazon S3 storage support
* ceph – provides common part of Ceph storage support
* ceph_recommended_lib – provides Ceph (>=0.80) storage support
* ceph_alternative_lib – provides Ceph (>=12.2.0) storage support
* file – provides file driver support
* redis – provides Redis storage support
* doc – documentation building support
* test – unit and functional tests support

To install Gnocchi from source, run the standard Python installation
procedure::

  pip install -e .

Again, depending on the drivers and features you want to use, you need to
install extra variants using, for example::

  pip install -e .[postgresql,ceph,ceph_recommended_lib]


Ceph requirements
-----------------

The ceph driver needs to have a Ceph user and a pool already created. They can
be created for example with:

::

    ceph osd pool create metrics 8 8
    ceph auth get-or-create client.gnocchi mon "allow r" osd "allow rwx pool=metrics"


Gnocchi leverages some librados features (omap, async, operation context)
available in python binding only since python-rados >= 12.2.0. To handle this,
Gnocchi uses 'cradox' python library which has exactly the same API but works
with Ceph >= 0.80.0.

If Ceph and python-rados are >= 12.2.0, cradox python library becomes optional
but is still recommended.


Configuration
=============

Configuration file
-------------------

By default, gnocchi looks for its configuration file in the following places,
in order:

* ``~/.gnocchi/gnocchi.conf``
* ``~/gnocchi.conf``
* ``/etc/gnocchi/gnocchi.conf``
* ``/etc/gnocchi.conf``
* ``~/gnocchi/gnocchi.conf.d``
* ``~/gnocchi.conf.d``
* ``/etc/gnocchi/gnocchi.conf.d``
* ``/etc/gnocchi.conf.d``


No config file is provided with the source code; it will be created during the
installation. In case where no configuration file was installed, one can be
easily created by running:

::

    gnocchi-config-generator > /path/to/gnocchi.conf

Configure Gnocchi by editing the appropriate file.

The configuration file should be pretty explicit, but here are some of the base
options you want to change and configure:

+---------------------+---------------------------------------------------+
| Option name         | Help                                              |
+=====================+===================================================+
| storage.driver      | The storage driver for metrics.                   |
+---------------------+---------------------------------------------------+
| indexer.url         | URL to your indexer.                              |
+---------------------+---------------------------------------------------+
| storage.file_*      | Configuration options to store files              |
|                     | if you use the file storage driver.               |
+---------------------+---------------------------------------------------+
| storage.swift_*     | Configuration options to access Swift             |
|                     | if you use the Swift storage driver.              |
+---------------------+---------------------------------------------------+
| storage.ceph_*      | Configuration options to access Ceph              |
|                     | if you use the Ceph storage driver.               |
+---------------------+---------------------------------------------------+
| storage.s3_*        | Configuration options to access S3                |
|                     | if you use the S3 storage driver.                 |
+---------------------+---------------------------------------------------+
| storage.redis_*     | Configuration options to access Redis             |
|                     | if you use the Redis storage driver.              |
+---------------------+---------------------------------------------------+

The same options are also available as `incoming.<drivername>_*` for
configuring the incoming storage. If no incoming storage is set, the default is
to use the configured storage driver.

Configuring authentication
-----------------------------

The API server supports different authentication methods: `basic` (the default)
which uses the standard HTTP `Authorization` header or `keystone` to use
`OpenStack Keystone`_. If you successfully installed the `keystone` flavor
using `pip` (see :ref:`installation`), you can set `api.auth_mode` to
`keystone` to enable Keystone authentication.

.. _`Paste Deployment`: http://pythonpaste.org/deploy/
.. _`OpenStack Keystone`: http://launchpad.net/keystone

Initialization
==============

Once you have configured Gnocchi properly you need to initialize the indexer
and storage:

::

    gnocchi-upgrade


Upgrading
=========
In order to upgrade from a previous version of Gnocchi, you need to make sure
that your indexer and storage are properly upgraded.

.. warning::

   Upgrade is only supported between one major version to another or between
   minor versions, e.g.:

   - version 2.0 to version 2.1 or 2.2 is supported

   - version 2.1 to version 3.0 is supported

   - version 2 to version 4 is **not** supported.

Run the following:

1. Stop the old version of Gnocchi API server and `gnocchi-statsd` daemon

2. Stop the old version of `gnocchi-metricd` daemon

.. warning::

   Data in backlog is never migrated between versions. Ensure the backlog is
   empty before any upgrade to ensure data is not lost.

3. Install the new version of Gnocchi

4. Run `gnocchi-upgrade`.

   This will take from a few minutes to several hours depending on the size of
   your index and storage.

5. Start the new Gnocchi API server, `gnocchi-metricd`
   and `gnocchi-statsd` daemons


Installation Using Devstack
===========================

To enable Gnocchi in `devstack`_, add the following to local.conf:

::

    enable_plugin gnocchi https://github.com/gnocchixyz/gnocchi master

To enable Grafana support in devstack, you can also enable `gnocchi-grafana`::

    enable_service gnocchi-grafana

Then, you can start devstack:

::

    ./stack.sh

.. _devstack: http://devstack.org
