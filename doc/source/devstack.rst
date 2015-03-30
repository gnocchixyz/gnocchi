==========
 Devstack
==========

To enable Gnocchi in devstack, add the following to local.conf:

::

    enable_plugin gnocchi https://github.com/openstack/gnocchi master
    enable_service gnocchi-api


Then, you can start devstack:

::

    ./stack.sh

