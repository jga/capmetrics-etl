==============
capmetrics-etl
==============

Extract-Transform-Load for performance data from Austin's Capital Metro Transportation Authority.

Documentation
-------------

In-depth documentation for **capmetrics-etl** can be found at its
`Read The Docs page <http://capmetrics-etl.readthedocs.io/en/latest/>`_.

Testing
-------

If you want to further develop **capmetrics-etl**, you are strongly encouraged to use ``pytest``.

Make sure to place ``pytest`` in your Python environment/virtual environment and the SQLite database
installed in your system. To run the entire test suite, make this call from the command line::

    $ py.test tests

A few tests take a bit of time to finish, so be patient if running the full suite.