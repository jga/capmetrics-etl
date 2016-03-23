.. capmetrics-etl documentation master file, created by
   sphinx-quickstart on Mon Jan 11 00:08:57 2016.

capmetrics-etl
==============

Data Availability
-----------------

In February of 2016, the Capital Metro Transit Authority - which serves
the central Texas region - began releasing a semi-annually updated
ridership statistics spreadsheet to the public. The spreadsheet
is an Excel file with in `xls` format.

The spreadsheets are placed on the `CapMetro stats page <http://capmetro.org/stats/>`_.
You may have to look around the page to find the Excel (`xls` format) file with the data.

File Contents
-------------

The current version of the Excel spreadsheet released contains seven worksheets. These
worksheets are designed with an arbitrary structure aimed at human readability.

Below is a listing of tables and the relevant data **capmetrics-etl** targets for extraction.

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

Models
------

Route - id, number, route name, service type

DailyRidership - route, day of week, period

HourlyRidership - route, day of week, period

Data Quality
------------

Given the absence of a reporting standard or specification by which the statistics file
is developed, **capmetrics-etl** runs a series of 'sanity checks' to ensure that the
data format expectations upon which its ETL procedures are based are actually in place.

Check for the presence of the six worksheets from which data is extracted.

Check for first column with route numbers in the 3 daily ridership worksheets.



.. toctree::
   :maxdepth: 2

   quality

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`

