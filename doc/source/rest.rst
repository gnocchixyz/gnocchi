================
 REST API Usage
================

Authentication
==============

By default, the `api.middleware` configuration option is set to use the Keystone
middleware. Therefore you must authenticate using Keystone to use the API and
provide an `X-Auth-Token` header with a valid token for each request sent to
Gnocchi.

Metrics
=======

Gnocchi provides a resource type that is called *metric*. A metric designates
any thing that can be measured: the CPU usage of a server, the temperature of a
room or the number of bytes sent by a network interface.

A metric only has a few properties: a UUID to identify it, and the archive
policy that will be used to store and aggregate the measures.

To create a metric, the following API request should be used:

::

  ▶ POST /v1/metric
    Content-Type: application/json

    {
      "user_id": "525CDB19-633B-4BCB-B928-9597BFED389F",
      "project_id": "26B1BC1F-B981-46F4-ADBF-FAD65336F1BA",
      "archive_policy": "medium"
    }

  ◀ HTTP/1.1 201 Created
    Location: http://localhost:8080/v1/metric/125F6A9F-D8DB-424D-BFF2-A5F142E2DC03
    Content-Type: application/json

    {
      "user_id": "525CDB19-633B-4BCB-B928-9597BFED389F",
      "project_id": "26B1BC1F-B981-46F4-ADBF-FAD65336F1BA",
      "archive_policy": "medium"
    }


By default, the `user_id` and `project_id` are set to the one used to
authenticate against the API, but you can also specify them.

Once created, you can retrieve the metric information:

::

  ▶ GET /v1/metric/125F6A9F-D8DB-424D-BFF2-A5F142E2DC03

  ◀ HTTP/1.1 200 Ok
    Content-Type: application/json

    {
      "user_id": "525CDB19-633B-4BCB-B928-9597BFED389F",
      "project_id": "26B1BC1F-B981-46F4-ADBF-FAD65336F1BA",
      "archive_policy": "medium"
    }

You can retrieve the archive policy definitions by adding a *details* parameter
to this request:

::

  ▶ GET /v1/metric/125F6A9F-D8DB-424D-BFF2-A5F142E2DC03?details=true

  ◀ HTTP/1.1 200 Ok
    Content-Type: application/json

    {
      "user_id": "525CDB19-633B-4BCB-B928-9597BFED389F",
      "project_id": "26B1BC1F-B981-46F4-ADBF-FAD65336F1BA",
      "archive_policy": {
        "back_window": 0,
        "definition": [
          {
            "points": 60,
            "timespan": "1:00:00",
            "granularity": "0:01:00"
          },
          {
            "points": 168,
            "timespan": "7 days, 0:00:00",
            "granularity": "1:00:00"
          },
          {
            "points": 365,
            "timespan": "365 days, 0:00:00",
            "granularity": "1 day, 0:00:00"
          }
        ],
        "name": "medium"
      }
    }

It is also possible to send the *details* parameter in the *Accept* header:

::

  ▶ GET /v1/metric/125F6A9F-D8DB-424D-BFF2-A5F142E2DC03
    Accept: application/json; details=true

  ◀ HTTP/1.1 200 Ok
    Content-Type: application/json

    {
      "user_id": "525CDB19-633B-4BCB-B928-9597BFED389F",
      "project_id": "26B1BC1F-B981-46F4-ADBF-FAD65336F1BA",
      "archive_policy": {
        "definition": [
          {
            "points": 60,
            "timespan": "1:00:00",
            "granularity": "0:01:00"
          },
          {
            "points": 168,
            "timespan": "7 days, 0:00:00",
            "granularity": "1:00:00"
          },
          {
            "points": 365,
            "timespan": "365 days, 0:00:00",
            "granularity": "1 day, 0:00:00"
          }
        ],
        "name": "medium"
      }
    }

It is possible to send metrics to the metric:

::

  ▶ POST /v1/metric/125F6A9F-D8DB-424D-BFF2-A5F142E2DC03/measures
    Content-Type: application/json

    [
      {
        "timestamp": "2014-10-06T14:33:57",
        "value": 43.1
      },
      {
        "timestamp": "2014-10-06T14:34:12",
        "value": 12
      }
      {
        "timestamp": "2014-10-06T14:34:20",
        "value": 2
      }
    ]

  ◀ HTTP/1.1 204 No Content

If there are no errors, Gnocchi does not return a response body, only a simple
status code. It is possible to provide any number of measures.

.. IMPORTANT::

   While it is possible to send any number of (timestamp, value), it is still
   needed to honor constraints defined by the archive policy used by the metric,
   such as the maximum timespan.


Once measures are sent, it is possible to retrieve them using *GET* on the same
endpoint:

::

  ▶ GET /v1/metric/125F6A9F-D8DB-424D-BFF2-A5F142E2DC03/measures

  ◀ HTTP/1.1 200 OK
    Content-Type: application/json

    [
      ["2014-10-06T14:30:00", 300.0, 22.7],
      ["2014-10-06T14:33:00", 60.0, 43.1],
      ["2014-10-06T14:34:00", 60.0, 7]
    ]

The list of points returned is composed of tuples with (timestamp, granularity,
value) sorted by timestamp. The granularity is the timespan covered by
aggregation for this point.

It is possible to filter the measures over a time range by specifying the
*start* and/or *stop* parameters to the query with timestamp. The timestamp
format can be either a floating number (UNIX epoch) or an ISO8601 formated
timestamp:

::

  ▶ GET /v1/metric/125F6A9F-D8DB-424D-BFF2-A5F142E2DC03/measures?start=2014-10-06T14:34

  ◀ HTTP/1.1 200 OK
    Content-Type: application/json

    [
      ["2014-10-06T14:34:00", 60.0, 7]
    ]

By default, the aggregated values that are returned use the *mean* aggregation
method. It is possible to request for any other method by specifying the
*aggregation* query parameter:

::

  ▶ GET /v1/metric/125F6A9F-D8DB-424D-BFF2-A5F142E2DC03/measures?aggregation=max

  ◀ HTTP/1.1 200 OK
    Content-Type: application/json

    [
      ["2014-10-06T14:33:00", 60.0, 43.1],
      ["2014-10-06T14:34:00", 60.0, 12]
    ]

The list of aggregation method available is: *mean*, *sum*, *last*, *max*,
*min*, *std*, *median* and *first*.

Archive Policy
==============

When sending measures for a metric to Gnocchi, the values are dynamically
aggregated. That means that Gnocchi does not store all sent measures, but
aggregates them over a certain period of time. Gnocchi provides several
aggregation methods (mean, min, max, sum…) that are builtin.

An archive policy is defined by a list of items in the `definition` field. Each
item is composed of the timespan and the level of precision that must be kept
when aggregating data, determined using at least 2 of the `points`,
`granularity` and `timespan` fields. For example, an item might be defined as 12
points over 1 hour (one point every 5 minutes), or 1 point every 1 hour over 1
day (24 points).

By default, new measures can only be processed if they have timestamps in the
future or part of the last aggregation period. The window size is based on the
largest granularity defined in the archive policy definition. To enlarge this
window size, the `back_window` field can be used to set the number of periods to
keep so it is possible to process measures that are older than the last
timestamp period boundary.

For example, if an archive policy is defined with coarsest aggregation of 1
hour, and the last point processed has a timestamp of 14:34, it's possible to
process measures back to 14:00 with a `back_window` of 0. If the `back_window`
is set to 2, it will be possible to send measures with timestamp back to 12:00.

The REST API allows to create archive policies this way:

::

  ▶ POST /v1/archive_policy
    Content-Type: application/json

    {
      "name": "low",
      "back_window": 0,
      "definition": [
        {
          "granularity": "1s",
          "timespan": "1 hour"
        },
        {
          "points": 1000,
          "timespan": "1 day"
        }
      ]
    }

  ◀ HTTP/1.1 201 Created
    Location: http://localhost:8080/v1/archive_policy/low
    Content-Type: application/json

    {
      "name": "low",
      "back_window": 0,
      "definition": [
        {
          "granularity": "0:00:01",
          "timespan": "0:01:00",
          "points": 60,
        },
        {
          "granularity": 86.4,
          "points": 1000,
          "timespan": "1 day, 00:00:00"
        }
      ]
    }

Once the archive policy is created, the complete set of properties is computed
and returned, with the URL of the archive policy. This URL can be used to
retrieve the details of the archive policy later:

::

  ▶ GET /v1/archive_policy/low

  ◀ HTTP/1.1 200 OK
    Location: http://localhost:8080/v1/archive_policy/low
    Content-Type: application/json

    {
      "name": "low",
      "back_window": 0,
      "definition": [
        {
          "granularity": "0:00:01",
          "timespan": "0:01:00",
          "points": 60,
        },
        {
          "granularity": 86.4,
          "points": 1000,
          "timespan": "1 day, 00:00:00"
        }
      ]
    }

It is also possible to list archive policies:

::

  ▶ GET /v1/archive_policy

  ◀ HTTP/1.1 200 OK
    Content-Type: application/json

    [
      {
        "name": "low",
        "back_window": 0,
        "definition": [
          {
            "granularity": "0:00:01",
            "timespan": "0:01:00",
            "points": 60,
          },
          {
            "granularity": 86.4,
            "points": 1000,
            "timespan": "1 day, 00:00:00"
          }
        ]
      }
    ]

.. WARNING::

   It is not yet possible to delete an archive policy.

Resources
=========

Gnocchi provides the ability to store and index resources. Each resource has a
type. The basic type of resources is *generic*, but more specialized subtypes
also exist, especially to describe OpenStack resources.

The REST API allows to manipulate resources. To create a generic resource:

::

  ▶ POST /v1/resource/generic
    Content-Type: application/json

    {
      "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
      "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D"
    }

  ◀ HTTP/1.1 201 Created
    Location: http://localhost:8080/v1/resource/generic/75C44741-CC60-4033-804E-2D3098C7D2E9
    Content-Type: application/json

    {
      "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
      "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "started_at": "2014-10-06T14:34:00",
      "ended_at": null
    }

The *id*, *user_id* and *project_id* attributes must be UUID and are mandatory.
The timestamp describing the lifespan of the resource are not, and *started_at*
is by default set to the current timestamp.

It's possible to retrieve the resource by the URL provided in the `Location`
header.

More specialized resources can be created. For example, the *instance* is used
to describe an OpenStack instance as managed by Nova_.

::

  ▶ POST /v1/resource/instance
    Content-Type: application/json

    {
      "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
      "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "flavor_id": 2,
      "image_ref": "http://image",
      "host": "compute1",
      "display_name": "myvm",
      "metrics": {}
    }

  ◀ HTTP/1.1 201 Created
    Location: http://localhost:8080/v1/resource/generic/75C44741-CC60-4033-804E-2D3098C7D2E9
    Content-Type: application/json

    {
      "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
      "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "flavor_id": 2,
      "image_ref": "http://image",
      "host": "compute1",
      "display_name": "myvm",
      "started_at": "2014-10-06T14:34:00",
      "ended_at": null,
      "metrics": {}
    }


All specialized types have their own optional and mandatory attributes, but they
all include attributes from the generic type as well.

To retrieve a resource by its URL provided by the `Location` header at creation
time:

::

  ▶ GET /v1/resource/generic/75C44741-CC60-4033-804E-2D3098C7D2E9

  ◀ HTTP/1.1 200 OK
    Content-Type: application/json

    {
      "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
      "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "flavor_id": 2,
      "image_ref": "http://image",
      "host": "compute1",
      "display_name": "myvm",
      "started_at": "2014-10-06T14:34:00",
      "ended_at": null,
      "metrics": {}
    }


It's possible to modify a resource by re-uploading it partially with the
modified fields:

::

  ▶ PATCH /v1/resource/generic/75C44741-CC60-4033-804E-2D3098C7D2E9
    Content-Type: application/json

    {
      "host": "compute2",
    }


  ◀ HTTP/1.1 201 Created
    Location: http://localhost:8080/v1/resource/generic/75C44741-CC60-4033-804E-2D3098C7D2E9
    Content-Type: application/json

    {
      "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
      "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "flavor_id": 2,
      "image_ref": "http://image",
      "host": "compute2",
      "display_name": "myvm",
      "started_at": "2014-10-06T14:34:00",
      "ended_at": null,
      "metrics": {}
    }


It possible to delete a resource altogether:

::

  ▶ DELETE /v1/resource/generic/75C44741-CC60-4033-804E-2D3098C7D2E9

  ◀ HTTP/1.1 204 No Content


All resources can be listed, either by using the `generic` type that will list
all types of resources, or by filtering on their resource type:

::

  ▶ GET /v1/resource/generic

  ◀ HTTP/1.1 200 OK
    Content-Type: application/json

    [
     {
       "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
       "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
       "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
       "type": "instance",
       "started_at": "2014-10-06T14:34:00",
       "ended_at": null,
       "metrics": {}
     },
     {
       "id": "63F07754-F52D-4321-A422-138D019E0EF1",
       "user_id": "763F8A05-16CF-42B0-B2C4-5E9A76D7781B",
       "project_id": "439AC15D-23BC-4589-9033-A98AAD4D00EE",
       "type": "swift_account",
       "started_at": "2014-10-06T14:34:00",
       "ended_at": null,
       "metrics": {}
     }
    ]


No attributes specific to the resource type are retrieved when using the
`generic` endpoint. To retrieve the details, either list using the specific
resource type endpoint:

::

  ▶ GET /v1/resource/instance

  ◀ HTTP/1.1 200 OK
    Content-Type: application/json

    [
     {
       "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
       "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
       "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
       "type": "instance",
       "flavor_id": 2,
       "image_ref": "http://image",
       "host": "compute1",
       "display_name": "myvm",
       "started_at": "2014-10-06T14:34:00",
       "ended_at": null,
       "metrics": {}
     }
    ]


or using `details=true` in the query parameter:

::

  ▶ GET /v1/resource/generic?details=true

  ◀ HTTP/1.1 200 OK
    Content-Type: application/json

    [
     {
       "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
       "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
       "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
       "type": "instance",
       "flavor_id": 2,
       "image_ref": "http://image",
       "host": "compute1",
       "display_name": "myvm",
       "started_at": "2014-10-06T14:34:00",
       "ended_at": null,
       "metrics": {}
     },
     {
       "id": "63F07754-F52D-4321-A422-138D019E0EF1",
       "user_id": "763F8A05-16CF-42B0-B2C4-5E9A76D7781B",
       "project_id": "439AC15D-23BC-4589-9033-A98AAD4D00EE",
       "type": "swift_account",
       "started_at": "2014-10-06T14:34:00",
       "ended_at": null,
       "metrics": {}
     }
    ]

When listing resources, it is possible to filter resource based on attributes
values:

::

  ▶ GET /v1/resource/instance?user_id=BD3A1E52-1C62-44CB-BF04-660BD88CD74D

  ◀ HTTP/1.1 200 OK
    Content-Type: application/json

    [
     {
       "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
       "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
       "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
       "type": "instance",
       "flavor_id": 2,
       "image_ref": "http://image",
       "host": "compute1",
       "display_name": "myvm",
       "started_at": "2014-10-06T14:34:00",
       "ended_at": null,
       "metrics": {}
     }
    ]

Each resource can be linked to any number of metrics. The `metrics` attributes
is a key/value field where the key is the name of the relationship and the value
is a metric:

::

  ▶ POST /v1/resource/instance
    Content-Type: application/json

    {
      "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
      "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "flavor_id": 2,
      "image_ref": "http://image",
      "host": "compute1",
      "display_name": "myvm",
      "metrics": {"cpu.util": "73CFA91B-F868-4FC1-BA6B-9164570AEAA1"}
    }

  ◀ HTTP/1.1 201 Created
    Location: http://localhost:8080/v1/resource/generic/75C44741-CC60-4033-804E-2D3098C7D2E9
    Content-Type: application/json

    {
      "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
      "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "flavor_id": 2,
      "image_ref": "http://image",
      "host": "compute1",
      "display_name": "myvm",
      "started_at": "2014-10-06T14:34:00",
      "ended_at": null,
      "metrics": {"cpu.util": "73CFA91B-F868-4FC1-BA6B-9164570AEAA1"}
    }

It's also possible to create metrics dynamically while creating a resource:

::

  ▶ POST /v1/resource/instance
    Content-Type: application/json

    {
      "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
      "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "flavor_id": 2,
      "image_ref": "http://image",
      "host": "compute1",
      "display_name": "myvm",
      "metrics": {"cpu.util": {"archive_policy": "medium"}}
    }

  ◀ HTTP/1.1 201 Created
    Location: http://localhost:8080/v1/resource/generic/75C44741-CC60-4033-804E-2D3098C7D2E9
    Content-Type: application/json

    {
      "id": "75C44741-CC60-4033-804E-2D3098C7D2E9",
      "user_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "project_id": "BD3A1E52-1C62-44CB-BF04-660BD88CD74D",
      "flavor_id": 2,
      "image_ref": "http://image",
      "host": "compute1",
      "display_name": "myvm",
      "started_at": "2014-10-06T14:34:00",
      "ended_at": null,
      "metrics": {"cpu.util": "2B9D2EAD-E14D-40C8-B50A-A94841F64D92"}
    }


The metric associated with a resource an be accessed and manipulated using the
usual `/v1/metric` endpoint or using the named relationship with the resource:

::

  ▶ GET /v1/resource/generic/75C44741-CC60-4033-804E-2D3098C7D2E9/metric/cpu.util/measures?start=2014-10-06T14:34

  ◀ HTTP/1.1 200 OK
    Content-Type: application/json

    [
      ["2014-10-06T14:34:00", 60.0, 7]
    ]

The same endpoint can be used to append metrics to a resource:

::

  ▶ POST /v1/resource/generic/75C44741-CC60-4033-804E-2D3098C7D2E9/metric
    Content-Type: application/json

    [
     {"memory": {"archive_policy": "low"}}
    ]

  ◀ HTTP/1.1 204 No Content


.. _Nova: http://launchpad.net/nova
