========
Glossary
========

.. glossary::
   :sorted:

   Resource
     An entity representing anything in your infrastructure that you will
     associate |metric|\ (s) with. It is identified by a unique ID and can
     contain attributes.

   Metric
     An entity storing |aggregates| identified by an UUID. It can be attached
     to a |resource| using a name. How a metric stores its |aggregates| is
     defined by the |archive policy| it is associated to.

   Measure
     An incoming datapoint tuple sent to Gnocchi by the api. It is composed
     of a timestamp and a value.

   Archive policy
     An |aggregate| storage policy attached to a |metric|. It determines how
     long |aggregates| will be kept in a |metric| and
     :term:`how they will be aggregated<aggregation method>`\ .

   Granularity
     The time between two |aggregates| in an aggregated |time series| of a
     |metric|.

   Time series
     A list of |aggregates| ordered by time.

   Aggregation method
     Function used to aggregate multiple |measures| into an |aggregate|. For
     example, the `min` aggregation method will aggregate the values of
     different |measures| to the minimum value of all the |measures| in the
     time range.

   Aggregate
     A datapoint tuple generated from several |measures| according to the
     |archive policy| definition. It is composed of a timestamp and a value.

   Timespan
     The time period for which a |metric| keeps its |aggregates|. It is used in
     the context of |archive policy|.

.. include:: include/term-substitution.rst
