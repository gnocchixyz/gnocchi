========
 Client
========

Python
------

Gnocchi officially provides a Python client and SDK which can be installed
using *pip*::

  pip install gnocchiclient

This package provides the `gnocchi` command line tool that can be used to send
requests to Gnocchi. You can read the `full documentation online`_.

Go
--

There is an open source Go implementation for the SDK, provided by the
`Gophercloud` project.
It can be installed using *go get*::

  go get github.com/gophercloud/utils/gnocchi

This package provides the Go SDK only. You can read the `godoc reference`_.

.. _full documentation online: http://gnocchi.xyz/gnocchiclient
.. _Gophercloud: https://github.com/gophercloud
.. _godoc reference: https://godoc.org/github.com/gophercloud/utils
