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

.. _`mod_wsgi`: https://modwsgi.readthedocs.org/en/master/
.. _`pecan deployment`: http://pecan.readthedocs.org/en/latest/deployment.html#deployment
