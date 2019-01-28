# csv
Download or upload csv files

This microservice
  * can be used to
    * upload json content to specified url
    * download csv content as json from specified url


### Important Notes
  * Port 5000 is used unless specified otherwise via environment variable _PORT_
  * All [SESAM reserved fields](https://docs.sesam.io/entitymodel.html?highlight=_ts#reserved-fields) except the ones specified in the _sesam_fields_wl_ parameter are omitted from the output csv upon upload
  * _transit_decode_  parameter can be used to remove the transit encoding
  * boolean query parameters reveals _True_ only if it the balue is "true" in case INsensitive manner
  * pandas library is used and all the _applicable_ features are applied with service specific defaults. see followings for and the defaults in the _service.py_ to figure out the full capability list
    * Download
      * http://pandas.pydata.org/pandas-docs/version/0.24/reference/api/pandas.read_csv.html
      * http://pandas.pydata.org/pandas-docs/version/0.24/reference/api/pandas.DataFrame.to_json.html
    * Upload
      * http://pandas.pydata.org/pandas-docs/version/0.24/reference/api/pandas.DataFrame.to_csv.html#pandas.DataFrame.to_csv


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
