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
* ceph_alternative_lib – provides Ceph (>=10.1.0) storage support
* file – provides file driver support
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
available in python binding only since python-rados >= 10.1.0. To handle this,
Gnocchi uses 'cradox' python library which has exactly the same API but works
with Ceph >= 0.80.0.

If Ceph and python-rados are >= 10.1.0, cradox python library becomes optional
but is still recommended.


Configuration
=============

Gnocchi is configured by the `/etc/gnocchi/gnocchi.conf` file.

No config file is provided with the source code; it will be created during the
installation. In case where no configuration file was installed, one can be
easily created by running:

::

    gnocchi-config-generator > /etc/gnocchi/gnocchi.conf

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
that your indexer and storage are properly upgraded. Run the following:

1. Stop the old version of Gnocchi API server and `gnocchi-statsd` daemon

2. Stop the old version of `gnocchi-metricd` daemon

3. Install the new version of Gnocchi

4. Run `gnocchi-upgrade`
   This can take several hours depending on the size of your index and
   storage.

5. Start the new Gnocchi API server, `gnocchi-metricd`
   and `gnocchi-statsd` daemons


Installation Using Devstack
===========================

To enable Gnocchi in `devstack`_, add the following to local.conf:

::

    enable_plugin gnocchi https://github.com/openstack/gnocchi master

To enable Grafana support in devstack, you can also enable `gnocchi-grafana`::

    enable_service gnocchi-grafana

Then, you can start devstack:

::

    ./stack.sh

.. _devstack: http://devstack.org
