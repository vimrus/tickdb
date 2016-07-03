# TickDB: A Timeseries Database

TickDB is a database for recording  and querying time series data.

##Features

 * Built-in HTTP API
 * Indexing for group(second, minute, day ...)
 * Easy to install and run

## Installation

    go get github.com/vimrus/tickdb

## Quick Start

### Create database
```
curl -XPUT 'http://localhost:9527/testdb'
```
### Insert data
```
curl -XPOST 'http://localhost:9527/testdb' -d '[
    {"index":"index1", "time":"2016-08-28T21:24:00Z", "value":{"open": 10.1, "close": 10.2}}
]'
```
### Get data
```
curl 'http://localhost:9527/testdb/index1/open/2016-08-28T21:24:00Z'
```

### Build query
```
curl http://localhost:9527/testdb/_query -d '
{
    "index": "index1",
    "from":"2016-08-00T08:00:00Z",
    "to":"2016-08-31T18:00:59Z",
    "group": "2minutes",
    "fields":{
        "open": {"reducer":"avg"}
    }
}'
```

### Delete data
```
curl -XDELETE "http://localhost:9527/testdb/index1" -d '
{
"from":"2016-08-00T08:00:00Z",
"to":"2016-08-31T18:00:59Z"
}'
```
