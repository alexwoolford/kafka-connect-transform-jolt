# kafka-connect-transform-jolt

This is a Kafka Connect single message transform (SMT) that allows us to manipulate JSON.

To create a Jolt transformation, go to the [Jolt Transform Demo site](https://jolt-demo.appspot.com/), look at the examples, and then experiment with your own sample data.

Here's an example transform:

Connect properties:

    "transforms": "jolt",
    "transforms.jolt.type": "io.woolford.JoltTransform$Value",
    "transforms.jolt.jolt.spec": "[{\"operation\":\"shift\",\"spec\":{\"RESP_LOCATION\":{\"LOCATION\":{\"LON\":\"lon\",\"LAT\":\"lat\"}},\"ID_ORIG_H\":\"ip_orig\",\"TS\":\"ts\",\"ID_RESP_H\":\"ip_resp\",\"UID\":\"uid\"}},{\"operation\":\"shift\",\"spec\":{\"lon\":\"location.lon\",\"lat\":\"location.lat\",\"ts\":\"ts\",\"ip_orig\":\"ip_orig\",\"ip_resp\":\"ip_resp\",\"uid\":\"uid\"}}]",

Input:

    {
      "TS": 1603161082983,
      "UID": "CslSx83mieZIJISXql",
      "ID_ORIG_H": "10.0.1.61",
      "ID_ORIG_P": 54005,
      "ID_RESP_H": "50.205.244.36",
      "ID_RESP_P": 123,
      "PROTO": "udp",
      "DURATION": 0.04233503341674805,
      "ORIG_BYTES": 48,
      "RESP_BYTES": 48,
      "CONN_STATE": "SF",
      "LOCAL_ORIG": true,
      "LOCAL_RESP": false,
      "MISSED_BYTES": 0,
      "HISTORY": "Dd",
      "ORIG_PKTS": 1,
      "ORIG_IP_BYTES": 76,
      "RESP_PKTS": 1,
      "RESP_IP_BYTES": 76,
      "ORIG_CC": null,
      "ORIG_CITY": null,
      "ORIG_REGION": null,
      "ORIG_LAT": null,
      "ORIG_LONG": null,
      "ORIG_LOCATION": {
        "CITY": null,
        "COUNTRY": null,
        "SUBDIVISION": null,
        "LOCATION": {
          "LON": null,
          "LAT": null
        }
      },
      "RESP_LOCATION": {
        "CITY": "Decatur",
        "COUNTRY": "United States",
        "SUBDIVISION": "Georgia",
        "LOCATION": {
          "LON": -84.2951,
          "LAT": 33.7657
        }
      }
    }

Transform:

    [
      {
        "operation": "shift",
        "spec": {
          "RESP_LOCATION": { "LOCATION": { "LON": "lon", "LAT": "lat" } },
          "ID_ORIG_H": "ip_orig",
          "TS": "ts",
          "ID_RESP_H": "ip_resp",
          "UID": "uid"
        }
      },
      {
        "operation": "shift",
        "spec": {
          "lon": "location.lon",
          "lat": "location.lat",
          "ts": "ts",
          "ip_orig": "ip_orig",
          "ip_resp": "ip_resp",
          "uid": "uid"
        }
      }
    ]

Output:

    {
      "location" : {
        "lon" : -84.2951,
        "lat" : 33.7657
      },
      "ts" : 1603161082983,
      "ip_orig" : "10.0.1.61",
      "ip_resp" : "50.205.244.36",
      "uid" : "CslSx83mieZIJISXql"
    }

