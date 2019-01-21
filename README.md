# csv
Download or upload csv files

This microservice
  * can be used to
    * upload json content to specified url
    * download csv content as json from specified url
  * listens on port 5000 unless specified otherwise via environment variable _PORT_
  * uses pandas library and offering all the _applicable_ features of the library.


 ### example config in SESAM
 system:
 ```
 {
    "_id": "my-csv-proxy",
    "type": "system:microservice",
    "connect_timeout": 60,
    "docker": {
        "environment": {
            "LOGLEVEL": "DEBUG"
        },
        "image": "sesamcommunity/csv:v1.0",
        "port": 5000
    },
    "read_timeout": 7200
}
```
pipe:
 ```
 {
    "_id": "company-freshdesk-ftp-endpoint-get-csv+ftp",
    "type": "pipe",
    "source": {
        "type": "json",
        "system": "my-csv-proxy",
        "url": "/download?url=http://some-server/some-path/somefile.csv"
    },
    "transform": {
        "type": "dtl",
        "rules": {
            "default": [
                ["add", "_id", "_S.id_field"],
                ["copy", "*"]
            ]
        }
    }
}

```
