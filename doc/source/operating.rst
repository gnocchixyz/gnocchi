===============
Running Gnocchi
===============

Once Gnocchi is properly installed, you need to launch Gnocchi. Simply run the
HTTP server and metric daemon:

::

    gnocchi-api
    gnocchi-metricd

You can run these services as background daemons.

Running API As A WSGI Application
=================================

To run Gnocchi API, you can use the provided `gnocchi-api`. It wraps around
`uwsgi` – makes sure that `uWSGI`_ is installed. If one Gnocchi API server is
not enough, you can spawn any number of new API server to scale Gnocchi out,
even on different machines.

Since Gnocchi API tier runs using WSGI, it can alternatively be run using
`Apache httpd`_ and `mod_wsgi`_, or any other HTTP daemon.

uWSGI
-----

If you want to deploy using `uWSGI`_ yourself, the following uWSGI
configuration file can be used as a base::

  [uwsgi]
  http = localhost:8041
  # Set the correct path depending on your installation
  wsgi-file = /usr/local/bin/gnocchi-api
  master = true
  die-on-term = true
  threads = 32
  # Adjust based on the number of CPU
  processes = 32
  enabled-threads = true
  thunder-lock = true
  plugins = python
  buffer-size = 65535
  lazy-apps = true
  add-header = Connection: close

You should configure the number of processes according to the number of CPU you
have, usually around 1.5 × number of CPU.

Once written to `/etc/gnocchi/uwsgi.ini`, it can be launched this way::

  uwsgi /etc/gnocchi/uwsgi.ini

Apache mod_wsgi
---------------

If you want to use Apache httpd `mod_wsgi`_, here's an example configuration
file::

  <VirtualHost *:8041>
    WSGIDaemonProcess gnocchi user=gnocchi processes=4 threads=32 display-name=%{GROUP}
    WSGIProcessGroup gnocchi
    WSGIScriptAlias / /usr/local/bin/gnocchi-api
    WSGIPassAuthorization On
    WSGIApplicationGroup %{GLOBAL}

    <Directory />
        Require all granted
    </Directory>
  </VirtualHost>


.. _Apache httpd: http://httpd.apache.org/
.. _mod_wsgi: https://modwsgi.readthedocs.org/
.. _uWSGI: https://uwsgi-docs.readthedocs.org/

How to define archive policies
==============================

The |archive policies| define how the |metrics| are aggregated and how long
they are stored. Each |archive policy| definition is expressed as the number of
points over a |timespan|.

If your |archive policy| defines a policy of 10 points with a |granularity| of
1 second, the |time series| archive will keep up to 10 seconds, each
representing an aggregation over 1 second. This means the |time series| will at
maximum retain 10 seconds of data between the more recent point and the oldest
point. That does not mean it will be 10 consecutive seconds: there might be a
gap if data is fed irregularly.

**There is no expiry of data relative to the current timestamp. Data is only
expired according to timespan.**

Each |archive policy| also defines which |aggregation methods| will be used.
The default is set to `default_aggregation_methods` which is by default set to
*mean*, *min*, *max*, *sum*, *std*, *count*.

Therefore, both the |archive policy| and the |granularity| entirely depends on
your use case. Depending on the usage of your data, you can define several
|archive policies|. A typical low grained use case could be::

    1440 points with a granularity of 1 minute = 24 hours

The worst case scenario for storing compressed data points is 8.04 bytes per
point, whereas best case scenario can compress up to 0.05 bytes per point.
Knowing that, it is possible to compute the worst case scenario for storage in
order to plan for data storage capacity.

An archive policy of 1440 points would need 1440 points × 8.04 bytes = 11.3 KiB
per |aggregation method|. If you use the 6 standard |aggregation method|
proposed by Gnocchi, your |metric| will take up to 6 × 11.3 KiB = 67.8 KiB of
disk space per metric.

Be aware that the more definitions you set in an |archive policy|, the more CPU
it will consume. Therefore, creating an |archive policy| with 2 definitons
(e.g. 1 second granularity for 1 day and 1 minute granularity for 1 month) may
consume twice CPU than just one definition (e.g. just 1 second granularity for
1 day).

Default archive policies
------------------------

By default, 4 |archive policies| are created when calling `gnocchi-upgrade`:
*bool*, *low*, *medium* and *high*. The name both describes the storage space
and CPU usage needs.

The `bool` |archive policy| is designed to store only boolean values (i.e. 0
and 1). It only stores one data point for each second (using the `last`
|aggregation method|), with a one year retention period. The maximum optimistic
storage size is estimated based on the assumption that no other value than 0
and 1 are sent as |measures|. If other values are sent, the maximum pessimistic
storage size is taken into account.

- low

  * 5 minutes granularity over 30 days
  * aggregation methods used: `default_aggregation_methods`
  * maximum estimated size per metric: 406 KiB

- medium

  * 1 minute granularity over 7 days
  * 1 hour granularity over 365 days
  * aggregation methods used: `default_aggregation_methods`
  * maximum estimated size per metric: 887 KiB

- high

  * 1 second granularity over 1 hour
  * 1 minute granularity over 1 week
  * 1 hour granularity over 1 year
  * aggregation methods used: `default_aggregation_methods`
  * maximum estimated size per metric: 1 057 KiB

- bool

  * 1 second granularity over 1 year
  * aggregation methods used: *last*
  * maximum optimistic size per metric: 1 539 KiB
  * maximum pessimistic size per metric: 277 172 KiB

How to plan for Gnocchi’s storage
=================================

Gnocchi uses a custom file format based on its library *Carbonara*. In Gnocchi,
a |time series| is a collection of points, where a point is a given |aggregate|
or sample, in the lifespan of a |time series|. The storage format is compressed
using various techniques, therefore the computing of a |time series|' size can
be estimated based on its **worst** case scenario with the following formula::

    number of points × 8 bytes = size in bytes

The number of points you want to keep is usually determined by the following
formula::

    number of points = timespan ÷ granularity

For example, if you want to keep a year of data with a one minute resolution::

    number of points = (365 days × 24 hours × 60 minutes) ÷ 1 minute
    number of points = 525 600

Then::

    size in bytes = 525 600 points × 8 bytes = 4 204 800 bytes = 4 106 KiB

This is just for a single aggregated |time series|. If your |archive policy|
uses the 6 default |aggregation methods| (mean, min, max, sum, std, count) with
the same "one year, one minute aggregations" resolution, the space used will go
up to a maximum of 6 × 4.1 MiB = 24.6 MiB.

Metricd
=======

Metricd is the daemon responsible for processing measures, computing their
aggregates and storing them into the aggregate storage. It also handles a few
other cleanup tasks, such as deleting metrics marked for deletion.

Metricd therefore is responsible for most of the CPU usage and I/O job in
Gnocchi. The archive policy of each metric will influence how fast it performs.

In order to process new measures, metricd checks the incoming storage for new
measures from time to time. The delay between each check is can be configured
by changing the `[metricd]metric_processing_delay` configuration option.

Some incoming driver (only Redis currently) are able to inform metricd that new
measures are available for processing. In that case, metricd will not respect
the `[metricd]metric_processing_delay` parameter and start processing the new
measures right away. This behaviour can be disabled by turning off the
`[metricd]greedy` option.

How many metricd workers do I need to run
-----------------------------------------

By default, `gnocchi-metricd` daemon spans all your CPU power in order to
maximize CPU utilisation when computing |metric| aggregation. You can use the
`gnocchi status` command to query the HTTP API and get the cluster status for
|metric| processing. It’ll show you the number of |metric| to process, known as
the processing backlog for `gnocchi-metricd`. As long as this backlog is not
continuously increasing, that means that `gnocchi-metricd` is able to cope with
the amount of |metric| that are being sent. In case this number of |measures|
to process is continuously increasing, you will need to (maybe temporarily)
increase the number of `gnocchi-metricd` daemons. You can run any number of
metricd daemon on any number of servers.

How to scale measure processing
-------------------------------

Measurement data pushed to Gnocchi is divided into "sacks" for better
distribution.  Incoming |metrics| are pushed to specific sacks and
each sack is assigned to one or more `gnocchi-metricd` daemons for
processing.

The number of sacks should be set based on the number of active
|metrics| the system will capture. Additionally, the number of sacks
should be higher than the total number of active `gnocchi-metricd`
workers.

In general, use the following equation to determine the appropriate `sacks`
value to set::

   sacks value = number of **active** metrics / 300

If the estimated number of |metrics| is the absolute maximum, divide
the value by 500 instead. If the estimated number of active |metrics|
is conservative and expected to grow, divide the value by 100 instead
to accommodate growth.

How do we change sack size
--------------------------

In the event your system grows to capture significantly more |metrics|
than originally anticipated, the number of sacks can be changed to
maintain good distribution. To avoid any loss of data when modifying
the number of `sacks`, the value should be changed in the following
order:

1. Stop all input services (api, statsd).

2. Stop all metricd services once backlog is cleared.

3. Run ``gnocchi-change-sack-size <number of sacks>`` to set new sack
   size. Note that the sack value can only be changed if the backlog
   is empty.

4. Restart all gnocchi services (api, statsd, metricd) with the new
   configuration.

Alternatively, to minimize API downtime:

1. Run gnocchi-upgrade but use a new incoming storage target such as a new
   ceph pool, file path, etc. Additionally, set |aggregate| storage to a
   new target as well.

2. Run ``gnocchi-change-sack-size <number of sacks>`` against the new
   target.

3. Stop all input services (api, statsd).

4. Restart all input services but target the newly created incoming
   storage.

5. When done clearing backlog from original incoming storage, switch
   all metricd daemons to target the new incoming storage but maintain
   original |aggregate| storage.

How to monitor Gnocchi
======================

The `/v1/status` endpoint of the HTTP API returns various information, such as
the number of |measures| to process (|measures| backlog), which you can easily
monitor (see `How many metricd workers do I need to run`_). The Gnocchi client
can show this output by running `gnocchi status`.

Making sure that the HTTP server and `gnocchi-metricd` daemon are running and
are not writing anything alarming in their logs is a sign of good health of the
overall system.

Total |measures| for backlog status may not accurately reflect the number of
points to be processed when |measures| are submitted via batch.

How to backup and restore Gnocchi
=================================

In order to be able to recover from an unfortunate event, you need to backup
both the index and the storage. That means creating a database dump (PostgreSQL
or MySQL) and doing snapshots or copy of your data storage (Ceph, S3, Swift or
your file system). The procedure to restore is no more complicated than initial
deployment: restore your index and storage backups, reinstall Gnocchi if
necessary, and restart it.

How to clear Gnocchi data
=========================

If you ever want to start fresh or need to clean Gnocchi data, this can be
easily done. You need to clean the measures (incoming), aggregates (storage)
and indexer data storage.

Once that is done, if you want to re-initialize Gnocchi, you need to call
`gnocchi-upgrade` so it re-initialize the different drivers.

Index storage
-------------

Both MySQL and PostgreSQL drivers uses a single database. Delete the database.
If you want to install Gnocchi again, create back that database with the same
name before calling `gnocchi-upgrade`.

Incoming data
-------------

Depending on the driver you use, the data are stored in different places:

* **Ceph**: delete the `gnocchi-config` object and the objects whose names
  start with `incoming` in the Ceph pool. Alternatively you can delete the Ceph
  pool (and recreate it if needed).
* **OpenStack Swift**: delete the `gnocchi-config` container and containers
  whose names start with `incoming` in the Swift account.
* **Redis**: delete the `gnocchi-config` key and the keys whose names start
  with `incoming`.
* **File**: delete `${incoming.file_basepath}/tmp` and the directories whose
  names start with `${incoming.file_basepath}/incoming`.
* **Amazon S3**: delete the bucket whose name start with `incoming`.

Storage data
------------

Depending on the driver you use, the data are stored in different places:

* **Ceph**: delete the objects whose names start with `gnocchi_` in the Ceph
  pool. Alternatively you can delete the Ceph pool (and recreate it if needed).
* **OpenStack Swift**: delete the containers whose names start with
  `$storage.swift_container_prefix` in the Swift account.
* **Redis**: delete the keys whose names start with `timeseries`.
* **File**: delete the directories whose names are UUIDs under
  `$incoming.file_basepath`.
* **Amazon S3**: delete the bucket whose name start with
  `$storage.s3_bucket_prefix`.

.. include:: include/term-substitution.rst
