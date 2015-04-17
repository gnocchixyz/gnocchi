==============
 Installation
==============

To install Gnocchi, you just need to run the standard Python installation
procedure:

::

    python setup.py install


Configuration
=============

Then configure Gnocchi by editing the `/etc/gnocchi/gnocchi.conf` sample. No
config file is provided with the source code, but one can be easily created by
running:

::

    tox -e genconfig

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

Indexer Initialization
======================

Once you have configured Gnocchi properly, you need to initialize the indexer:

::

    gnocchi-dbsync


Running Gnocchi
===============

To run Gnocchi, simple run the HTTP server:

::

    gnocchi-api


Running As A WSGI Application
=============================

It's possible – and strongly advised – to run Gnocchi through a WSGI
service such as `mod_wsgi`_ or any other WSGI application. The file
`gnocchi/rest/app.wsgi` provided with Gnocchi allows you to enable Gnocchi as
a WSGI application.
For other WSGI setup you can refer to the `pecan deployement`_ documentation.

.. _`mod_wsgi`: https://modwsgi.readthedocs.org/en/master/
.. _`pecan deployement`: http://pecan.readthedocs.org/en/latest/deployment.html#deployment
