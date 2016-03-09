===============
 Configuration
===============

Configure Gnocchi by editing `/etc/gnocchi/gnocchi.conf`.

No config file is provided with the source code; it will be created during the
installation. In case where no configuration file was installed, one can be
easily created by running:

::

    oslo-config-generator --config-file=/etc/gnocchi/gnocchi-config-generator.conf --output-file=/etc/gnocchi/gnocchi.conf

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

Gnocchi provides these indexer drivers:

- `PostgreSQL`_ (recommended)
- `MySQL`_

.. _`Swift`: https://launchpad.net/swift
.. _`Ceph`: http://ceph.com/
.. _`PostgreSQL`: http://postgresql.org
.. _`MySQL`: http://mysql.com

Configuring the WSGI pipeline
-----------------------------

The API server leverages `Paste Deployment`_ to manage its configuration. You
can edit the `/etc/gnocchi/api-paste.ini` to tweak the WSGI pipeline of the
Gnocchi REST HTTP server. By default, no authentication middleware is enabled,
meaning your request will have to provides the authentication headers.

Gnocchi is easily connectable with `OpenStack Keystone`_. If you successfully
installed the `keystone` flavor using `pip` (see :ref:`installation`), you can
edit the `api-paste.ini` file to add the Keystone authentication middleware::

  [pipeline:main]
  pipeline = gnocchi+auth

.. _`Paste Deployment`: http://pythonpaste.org/deploy/
.. _`OpenStack Keystone`: http://launchpad.net/keystone
.. _`CORS`: https://en.wikipedia.org/wiki/Cross-origin_resource_sharing
.. _`Grafana`: http://grafana.org/


Driver notes
============

Carbonara based drivers (file, swift, ceph)
-------------------------------------------

To ensure consistency across all *gnocchi-api* and *gnocchi-metricd* workers,
these drivers need a distributed locking mechanism. This is provided by the
'coordinator' of the `tooz`_ library.

By default, the configured backend for `tooz`_ is the same as the indexer
(*PostgreSQL* or *MySQL*). This allows locking across workers from different
nodes.

For a more robust multi-nodes deployment, the coordinator may be changed via
the `storage.coordination_url` configuration option to one of the other `tooz
backends`_.

For example, to use Redis backend::

    coordination_url = redis://<sentinel host>?sentinel=<master name>

or alternatively, to use the Zookeeper backend::

    coordination_url = zookeeper:///hosts=<zookeeper_host1>&hosts=<zookeeper_host2>

.. _`tooz`: http://docs.openstack.org/developer/tooz/
.. _`tooz backends`: http://docs.openstack.org/developer/tooz/drivers.html


Ceph driver implementation details
----------------------------------

Each batch of measurements to process is stored into one rados object.
These objects are named `measures_<metric_id>_<random_uuid>_<timestamp>`

Also a special empty object called `measure` has the list of measures to
process stored in its omap attributes.

Because of the asynchronous nature of how we store measurements in Gnocchi,
`gnocchi-metricd` needs to know the list of objects that are waiting to be
processed:

- Listing rados objects for this is not a solution since it takes too much
  time.
- Using a custom format into a rados object, would force us to use a lock
  each time we would change it.

Instead, the omaps of one empty rados object are used. No lock is needed to
add/remove an omap attribute.

Also xattrs attributes are used to store the list of aggregations used for a
metric. So depending on the filesystem used by ceph OSDs, xattrs can have
a limitation in terms of numbers and size if Ceph is not correctly configured.
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


Also Gnocchi can use `cradox`_ Python library if installed. This library is a
Python binding to librados written with `Cython`_, aiming to replace the one
written with `ctypes`_ provided by Ceph.
This new library will be part of next Ceph release (10.0.4).

The new Cython binding divides the gnocchi-metricd times to process measures
by a large factor.

So, if the Ceph installation doesn't use latest Ceph version, `cradox`_ can be
installed to improve the Ceph backend performance.


.. _`Ceph extended attributes documentation`: http://docs.ceph.com/docs/master/rados/configuration/filestore-config-ref/#extended-attributes
.. _`cradox`: https://pypi.python.org/pypi/cradox
.. _`Cython`: http://cython.org/
.. _`ctypes`: https://docs.python.org/2/library/ctypes.html
.. _`rados.py`: https://docs.python.org/2/library/ctypes.htm://github.com/ceph/ceph/blob/hammer/src/pybind/rados.py


Swift driver implementation details
-----------------------------------

The Swift driver leverages the bulk delete functionality provided by the bulk_
middleware to minimise the amount of requests made to clean storage data. This
middleware must be enabled to ensure Gnocchi functions correctly. By default,
Swift has this middleware enabled in its pipeline.

.. _bulk: http://docs.openstack.org/liberty/config-reference/content/object-storage-bulk-delete.html
