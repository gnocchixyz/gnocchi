===============
Running Gnocchi
===============

To run Gnocchi, simply run the HTTP server and metric daemon:

::

    gnocchi-api
    gnocchi-metricd


Running As A WSGI Application
=============================

It's possible – and strongly advised – to run Gnocchi through a WSGI
service such as `mod_wsgi`_ or any other WSGI application. The file
`gnocchi/rest/app.wsgi` provided with Gnocchi allows you to enable Gnocchi as
a WSGI application.
For other WSGI setup you can refer to the `pecan deployment`_ documentation.

.. _`pecan deployment`: http://pecan.readthedocs.org/en/latest/deployment.html#deployment


How to scale out the Gnocchi HTTP REST API tier
===============================================

The Gnocchi API tier runs using WSGI. This means it can be run using `Apache
httpd`_ and `mod_wsgi`_, or other HTTP daemon such as `uwsgi`_. You should
configure the number of process and threads according to the number of CPU you
have, usually around 1.5 × number of CPU. If one server is not enough, you can
spawn any number of new API server to scale Gnocchi out, even on different
machines.

.. _Apache httpd: http://httpd.apache.org/
.. _mod_wsgi: https://modwsgi.readthedocs.org/
.. _uwsgi: https://uwsgi-docs.readthedocs.org/


How many metricd workers do we need to run
==========================================

By default, `gnocchi-metricd` daemon spans all your CPU power in order to
maximize CPU utilisation when computing metric aggregation. You can use the
`gnocchi status` command to query the HTTP API and get the cluster status for
metric processing. It’ll show you the number of metric to process, known as the
processing backlog for `gnocchi-metricd`. As long as this backlog is not
continuously increasing, that means that `gnocchi-metricd` is able to cope with
the amount of metric that are being sent. In case this number of measure to
process is continuously increasing, you will need to (maybe temporarily)
increase the number of `gnocchi-metricd` daemons. You can run any number of
metricd daemon on any number of servers.

How to monitor Gnocchi
======================

The `/v1/status` endpoint of the HTTP API returns various information, such as
the number of measures to process (measures backlog), which you can easily
monitor (see `How many metricd workers do we need to run`_). Making sure that
the HTTP server and `gnocchi-metricd` daemon are running and are not writing
anything alarming in their logs is a sign of good health of the overall system.

Total measures for backlog status may not accurately reflect the number of
points to be processed when measures are submitted via batch.

How to backup and restore Gnocchi
=================================

In order to be able to recover from an unfortunate event, you need to backup
both the index and the storage. That means creating a database dump (PostgreSQL
or MySQL) and doing snapshots or copy of your data storage (Ceph, Swift or your
file system). The procedure to restore is no more complicated than initial
deployment: restore your index and storage backups, reinstall Gnocchi if
necessary, and restart it.
