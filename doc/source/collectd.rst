==================
 Collectd support
==================

`Collectd`_ can use Gnocchi to store its data through a plugin called
`collectd-gnocchi` or via the `gnocchi-amqp1d` daemon.


collectd-gnocchi
================

It can be installed with *pip*::

     pip install collectd-gnocchi

`Sources and documentation`_ are also available.


gnocchi-amqp1d
==============

You need first to setup the Collectd `amqp1 write plugin`::

    <Plugin amqp1>
      <Transport "name">
        Host "localhost"
        Port "5672"
        Address "collectd"
        <Instance "telemetry">
            Format JSON
        </Instance>
      </Transport>
    </Plugin>


Then configure the AMQP 1.0 url in gnocchi.conf::

    [amqp1d]
    url = localhost:5672/u/collectd/telemetry


.. _`Collectd`: https://www.collectd.org/
.. _`Sources and documentation`: https://github.com/gnocchixyz/collectd-gnocchi
.. _`amqp1 write plugin`: https://github.com/ajssmith/collectd/blob/d4cc32c4dddb01081c49a67d13ab4a737cda0ed0/src/collectd.conf.pod#plugin-amqp1
.. TODO(sileht): Change the link when
   https://collectd.org/documentation/manpages/collectd.conf.5.shtml will be
   up2date
