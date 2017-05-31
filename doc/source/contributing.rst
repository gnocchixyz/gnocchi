==============
 Contributing
==============

Issues
------

We use the `GitHub issue tracker`_ for reporting issues. Before opening a new
issue, ensure the bug was not already reported by searching on Issue tracker
first.

If you're unable to find an open issue addressing the problem, open a new one.
Be sure to include a title and clear description, as much relevant information
as possible, and a code sample or an executable test case demonstrating the
expected behavior that is not occurring.

.. _`GitHub issue tracker`: https://github.com/gnocchixyz/gnocchi/issues

Running the Tests
-----------------

Tests are run using `tox <https://tox.readthedocs.io/en/latest/>`_. Tox creates
a virtual environment for each test environment, so make sure you are using an
up to date version of `virtualenv <https://pypi.python.org/pypi/virtualenv>`_.

Different test environments and configurations can be found by running the
``tox -l`` command. For example, to run tests with Python 2.7, PostgreSQL as
indexer, and file as storage backend:

::

    tox -e py27-postgresql-file


To run tests with Python 2.7, MySQL as indexer, and Ceph as storage backend:

::

    tox -e py35-mysql-ceph
