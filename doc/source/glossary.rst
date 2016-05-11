========
Glossary
========

.. glossary::

   Resource
     An entity representing anything in your infrastructure that you will
     associate metric(s) with. It is identified by a unique ID and can contain
     attributes.

   Metric
     An entity storing measures identified by an UUID. It can be attached to a
     resource using a name. How a metric stores its measure is defined by the
     archive policy it is associated to.

   Measure
     A datapoint tuple composed of timestamp and a value.

   Archive policy
     A measure storage policy attached to a metric. It determines how long
     measures will be kept in a metric and how they will be aggregated.

   Granularity
     The time between two measures in an aggregated timeseries of a metric.

   Timeseries
     A list of measures.

   Aggregation method
     Function used to aggregate multiple measures in one. For example, the
     `min` aggregation method will aggregate the values of different measures
     to the minimum value of all the measures in time range.
