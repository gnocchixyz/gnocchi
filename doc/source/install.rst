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
| storage.driver      | The storage driver for metrics, Swift by default. |
+---------------------+---------------------------------------------------+
| indexer.driver      | The indexer driver, SQLAlchemy by default.        |
+---------------------+---------------------------------------------------+
| database.connection | URL to your database,                             |
|                     | used by the *sqlalchemy* driver.                  |
+---------------------+---------------------------------------------------+
| storage.swift_*     | Configuration options to access Swift             |
|                     | if you use the Swift storage driver.              |
+---------------------+---------------------------------------------------+


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

It's possible – and advised – to run Gnocchi through a WSGI service such as
`mod_wsgi`_ or any other WSGI applications. The file `gnocchi/rest/app.wsgi`
provided with Gnocchi allows you to enable Gnocchi as a WSGI application.

.. _`mod_wsgi`: https://modwsgi.readthedocs.org/en/master/

