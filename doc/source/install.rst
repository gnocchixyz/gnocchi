==============
 Installation
==============

Installation Using Devstack
===========================

To enable Gnocchi in devstack, add the following to local.conf:

::

    enable_plugin gnocchi https://github.com/openstack/gnocchi master
    enable_service gnocchi-api,gnocchi-metricd

To enable Grafana support in devstack, you can also enable `gnocchi-grafana`::

    enable_service gnocchi-grafana

Then, you can start devstack:

::

    ./stack.sh

Installation
============

To install Gnocchi using `pip`, just type::

  pip install gnocchi

Depending on the drivers you want to use, you need to install extra variants
using, for example::

  pip install gnocchi[postgresql,ceph]

To install Gnocchi from source, run the standard Python installation
procedure::

  pip install -e .

Again, depending on the drivers you want to use, you need to install extra
variants using, for example::

  pip install -e .[postgresql,ceph]
