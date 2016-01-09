=================
Grafana support
=================

`Grafana`_ has support for Gnocchi through a plugin. The repository named
`grafana-plugins`_ contains this plugin. You can enable the plugin by following
the instructions in the `Grafana documentation`_.

.. note::
   A `pull request`_ has been made to merge this plugin directly into Grafana
   main tree, but it has unfortunately being denied for the time being. Feel
   free to post a comment there requesting its reopening.

Grafana has 2 modes of operation: proxy or direct mode. In proxy mode, your
browser only communicates with Grafana, and Grafana communicates with Gnocchi.
In direct mode, your browser communicates with Grafana, Gnocchi, and possibly
Keystone.

Picking the right mode depends if your Gnocchi server is reachable by your
browser and/or by your Grafana server.

In order to use Gnocchi with Grafana in proxy mode, you just need to:

1. Install Grafana and its Gnocchi plugin
2. Configure a new datasource in Grafana with the Gnocchi URL.
   If you are using the Keystone middleware for authentication, you can also
   provide an authentication token.

In order to use Gnocchi with Grafana in direct mode, you need to do a few more
steps:

1. Enable the `CORS`_ middleware. This can be done easily by modifying the
   Gnocchi `api-paste.ini` configuration file and adding `cors` into the main
   pipeline::

     [pieline:main]
     pipeline = cors keystone_authtoken gnocchi

   This will authorize your browser to make requests to Gnocchi on behalf of
   Grafana.

2. Configure the CORS middleware in `gnocchi.conf` to allow request from
   Grafana::

     [cors]
     allowed_origin = http://example.com/grafana
     allow_headers = Content-Type,Cache-Control,Content-Language,Expires,Last-Modified,Pragma,X-Auth-Token

3. Configure the CORS middleware in Keystone in the same fashion.

4. Configure a new datasource in Grafana with the Keystone URL, a user, a
   project and a password. Your browser will query Keystone for a token, and
   then query Gnocchi based on what Grafana needs.

.. image:: grafana-screenshot.png
  :align: center
  :alt: Grafana screenshot

.. _`Grafana`: http://grafana.org
.. _`grafana-plugins`: https://github.com/grafana/grafana-plugins
.. _`pull request`: https://github.com/grafana/grafana/pull/2716
.. _`Grafana documentation`: http://docs.grafana.org/
.. _`CORS`: https://en.wikipedia.org/wiki/Cross-origin_resource_sharing
