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

If you are looking to contribute for the first time, some issues are tagged
with the "`good first issue`_" label and are easy targets for newcomers.

.. _`GitHub issue tracker`: https://github.com/gnocchixyz/gnocchi/issues
.. _`good first issue`: https://github.com/gnocchixyz/gnocchi/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22


Pull-requests
-------------

When opening a pull-request, make sure that:

* You write a comprehensive summary of your problem and the solution you
  implemented.
* If you update or fix your pull-request, make sure the commits are atomic. Do
  not include fix-up commits in your history, rewrite it properly using e.g.
  `git rebase --interactive` and/or `git commit --amend`.
* We recommend using `git pull-request`_ to send your pull-requests.

.. _`git pull-request`: https://github.com/jd/git-pull-request


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
