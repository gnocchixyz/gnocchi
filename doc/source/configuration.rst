===============
 Configuration
===============

Configure Gnocchi by editing `/etc/gnocchi/gnocchi.conf`.

No config file is provided with the source code, but one can be easily
created by running:

::

    tox -e genconfig

This command will create an `etc/gnocchi/gnocchi.conf` file which can be used
as a base for the default configuration file at `/etc/gnocchi/gnocchi.conf`. If
you're using *devstack*, this file is already generated and put in place.

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


Gnocchi provides these storage drivers:

- File (default)
- `Swift`_
- `Ceph`_
- `InfluxDB`_ (experimental)

Gnocchi provides these indexer drivers:

- `PostgreSQL`_ (recommended)
- `MySQL`_

.. _`Swift`: https://launchpad.net/swift
.. _`Ceph`: http://ceph.com/
.. _`PostgreSQL`: http://postgresql.org
.. _`MySQL`: http://mysql.com
.. _`InfluxDB`: http://influxdb.com


Driver notes
============

Carbonara based drivers (file, swift, ceph)
-------------------------------------------

To ensure consistency across all *gnocchi-api* and *gnocchi-metricd* workers,
these drivers need a distributed locking mechanism. This is provided by the
'coordinator' of the `tooz`_ library.

By default, the configured backend for `tooz`_ is `file`, this allows locking
across workers on the same node.

In a multi-nodes deployment, the coordinator needs to be changed via
the storage/coordination_url configuration options to one of the other
`tooz backends`_.

For example to use Redis backend::

    coordination_url = redis://<sentinel host>?sentinel=<master name>

or alternatively, to use the Zookeeper backend::

    coordination_url = zookeeper:///hosts=<zookeeper_host1>&hosts=<zookeeper_host2>

.. _`tooz`: http://docs.openstack.org/developer/tooz/
.. _`tooz backends`: http://docs.openstack.org/developer/tooz/drivers.html


Ceph driver implementation details
----------------------------------

Each batch of measurements to process is stored into one rados object.
These objects are named `measures_<metric_id>_<random_uuid>_<timestamp>`

Also a special empty object called `measures` has the list of measures to
process stored in its xattr attributes.

Because of the asynchronous nature of how we store measurements in Gnocchi,
`gnocchi-metricd` needs to know the list of objects that are waiting to be
processed:

- Listing rados objects for this is not a solution since it takes too much
  time.
- Using a custom format into a rados object, would force us to use a lock
  each time we would change it.

Instead, the xattrs of one empty rados object are used. No lock is needed to
add/remove a xattr.

But depending on the filesystem used by ceph OSDs, this xattrs can have a
limitation in terms of numbers and size if Ceph is not correctly configured.
See `Ceph extended attributes documentation`_ for more details.

Then, each Carbonara generated file is stored in *one* rados object.
So each metric has one rados object per aggregation in the archive policy.

Because of this, the filling of OSDs can look less balanced compared to RBD.
Some objects will be big and others small, depending on how archive policies
are set up.

We can imagine an unrealistic case such as retaining 1 point per second over
a year, in which case the rados object size will be ~384MB.

Whereas in a more realistic scenario, a 4MB rados object (like RBD uses) could
result from:

- 20 days with 1 point every second
- 100 days with 1 point every 5 seconds

So, in realistic scenarios, the direct relation between the archive policy and
the size of the rados objects created by Gnocchi is not a problem.

.. _`Ceph extended attributes documentation`: http://docs.ceph.com/docs/master/rados/configuration/filestore-config-ref/#extended-attributes
