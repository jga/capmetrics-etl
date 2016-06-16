===============
Getting started
===============

Overview
--------

To productively work with **capmetrics-etl**, you should understand:

1. What's in the data file released by CapMetro.

2. How to properly configure the required ``ini`` file.

3. The models that the application writes and updates.

4. What's under the hood of the application's commands.

This `Getting Started` guide explains all four.

To summarize, the data file is an Excel spreadsheet with several worksheets.  More detail on the file
layout and what **capmetrics-etl** does with it is below.

There are six models - five of them focus on transit operations data, and one captures metadata for
the ETL process.

The **capmetrics-etl** project has a couple of commands that can be run from a terminal/command line.

The ``capmetrics`` command checks a passed file for basic data quality and then proceeds to ingest its
data into the database specified by the configuration file. The ``capmetrics-tables`` command generates
database tables required for the models. You can read more details about each command in the sections below.

The Data File
-------------

In February of 2016, the Capital Metro Transit Authority - which serves
the central Texas region - began releasing a ridership statistics spreadsheet to the public.
The spreadsheet is an Excel file with the ``xls`` format.

The spreadsheets are placed on the `CapMetro stats page <http://capmetro.org/stats/>`_.
You may have to look around the page to find the Excel (``xls`` format) file with the data.
The current version of the Excel spreadsheet released contains seven worksheets, but only **six**
are consumed by **capmetrics-etl**. These worksheets are designed with an arbitrary structure
aimed at human readability.

Below is a listing of the worksheets and the relevant data **capmetrics-etl** targets for extraction.

Definitions table
.................

Provides a table of terms and definitions.  No data is extracted from this worksheet.

Ridership by Route Weekday
..........................

Table of route numbers, route names, and daily ridership counts per "weekday" for a
seasonal period (Fall, Spring, Summer).

The route numbers and route names are extracted.

The seasonal weekday ridership counts are extracted.

Ridership by Route Saturday
...........................

Table of route numbers, route names, and daily ridership counts per Saturday for a
seasonal period (Fall, Spring, Summer).

The route numbers and route names are extracted.

The seasonal weekday ridership counts are extracted.

Ridership by Route Sunday
.........................

Table of route numbers, route names, and daily ridership counts per Sunday for a
seasonal period (Fall, Spring, Summer).

The route numbers and route names are extracted.

The seasonal weekday ridership counts are extracted.

Riders per Hour Weekday
.......................

Table of route numbers, route names, and "weekday" ridership per hour data for a
seasonal period (Fall, Spring, Summer).

The seasonal weekday ridership per hour facts are extracted.

Riders Hour Saturday
....................

Table of route numbers, route names, and Saturday ridership per hour data for a
seasonal period (Fall, Spring, Summer).

The seasonal Saturday ridership per hour facts are extracted.

Riders per Hour Sunday
......................

Table of route numbers, route names, and Saturday ridership per hour data for a
seasonal period (Fall, Spring, Summer).

The seasonal Saturday ridership per hour facts are extracted.

Understanding the Models
------------------------

The six models that **capmetrics-etl** works with are ``Route``, ``DailyRidership``, ``ServiceHourProductivity``,
``SystemRidership``, ``SystemTrend``, and ``ETLReport``. The former five are for analyzing the performance data of CapMetro. The last one is purely for tracking
metadata on the universe of data crunched by **capmetrics-etl**.

The `ini` file
--------------

In order to run **capmetrics-etl** commands, you'll need an Python ``ini`` file. The application
expects the following entries:

**engine_url**

An SQL Alchemy URL for a database.

**daily_ridership_worksheets**

A list of the names of the daily ridership worksheets.

**hour_productivity_worksheets**

A list of the names of the service hour productivity worksheets.

Here is an example ``ini`` file with a PostgreSQL database configuration::

        [capmetrics]
        engine_url=postgresql://username:password@localhost:5432/cmxdb
        daily_ridership_worksheets=["Ridership by Route Weekday", "Ridership by Route Saturday", "Ridership by Route Sunday"]
        hour_productivity_worksheets=["Riders per Hour Weekday", "Riders Hour Saturday", "Riders per Hour Sunday"]

Make sure the database engine you select is properly configured on your machine. For example, if you
are using PostgreSQL, you'll need to have the appropriate drivers installed on your system, as well
as a Python driver such as ``psycopg`` in your Python virtual environment. For details, see the `psycopg2 guide`_.


Both the ``capmetrics`` and ``capmetrics-tables`` commands require that you pass the path to the ``ini`` file as the
first argument in the command call.

The ``capmetrics`` ETL command
------------------------------

Once you have **capmetrics-etl** installed, you run ETL with this call:

        $ capmetrics `data_file.xls` `capmetrics.ini`

The first argument is the path and name of the Excel data file. The second is the path
and name of the configuration file. Both are required.

Data Quality
............

Given the absence of a reporting standard or specification by which the statistics file
is developed, the logic of the ``capmetrics`` command runs a series of 'sanity checks' to ensure that the
data format expectations upon which its ETL procedures are based are actually in place
within the file.

The checks are:

1. Worksheet completeness - Check for the presence of all six of the worksheets from which data is extracted.

2. Route rows present - Check for at least one data point of route number and route name columns
   in the ridership worksheets.

3. Ridership columns present - Check for at least one ridership data column in all ridership data worksheets.

Build and Update Route models
.............................

After the data quality check, the logic for the ``capmetrics`` command ingests the route number-name pairings
and creates new route objects or updates the name and service type of existing ones.

Update ridership and trend data
...............................

After completing the update to ``Route`` models, the ``capmetrics`` command's logic updates ridership data and creates
new system ridership and trend data.

The ``capmetrics-tables`` command
---------------------------------

Once you have **capmetrics-etl** installed, you can create the necessary tables with this call:

        $ capmetrics-tables `capmetrics.ini`

The first argument is the path and name of a configruation file. You must supply a configuration file as
it is necessary to instantiate the SQL Alchemy engine that will create the tables for the application's models.

.. _psycopg2 guide: http://initd.org/psycopg/docs/install.html