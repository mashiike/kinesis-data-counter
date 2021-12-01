# kinesis-data-counter
![Latest GitHub release](https://img.shields.io/github/release/mashiike/kinesis-data-counter.svg)
![Github Actions test](https://github.com/mashiike/kinesis-data-counter/workflows/Test/badge.svg?branch=main)
[![Go Report Card](https://goreportcard.com/badge/mashiike/kinesis-data-counter)](https://goreportcard.com/report/mashiike/kinesis-data-counter) 
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/mashiike/kinesis-data-counter/blob/master/LICENSE)

kinesis data counting tool.   
want to count the Kinesis Data Stream where JSON data flows.  
## Install

### binary packages

[Releases](https://github.com/mashiike/kinesis-data-counter/releases).

### Homebrew tap

```console
$ brew install mashiike/tap/kinesis-data-counter
```

## Usage 

### as CLI command

```console
Usage of kinesis-data-counter:
  -config string
        kinesis-data-counter config
  -counter-id string
        set instant counter id [Only at CLI] (default "__instant__")
  -counter-query string
        set instant counter output query, jq expr [Only at CLI]
  -counter-target-column string
        set instant counter target column [Only at CLI] (default "*")
  -counter-type value
        set instant counter type [Only at CLI] (default count)
  -log-level string
        log level (default "info")
  -put record
        put record configured stream [Only at CLI]
  -stream string
        kinesis data stream name [Only at CLI]
  -window string
        tumbling window size, max 15m [Only at CLI]
```

quick start:
```console
$ kinesis-data-counter -window 1m -stream test-stream
2021/11/22 01:24:36 [info] start get record from arn:aws:kinesis:ap-northeast-1:111122223333:stream/test-stream
{"counter_id":"__instant__","counter_type":"count","event_source_arn":"arn:aws:kinesis:ap-northeast-1:111122223333:stream/test-stream","shard_id":"shardId-000000000000","value":203,"window_end":1637511900000,"window_start":1637511840000}
{"counter_id":"__instant__","counter_type":"count","event_source_arn":"arn:aws:kinesis:ap-northeast-1:111122223333:stream/test-stream","shard_id":"shardId-000000000000","value":502,"window_end":1637511960000,"window_start":1637511900000}
{"counter_id":"__instant__","counter_type":"count","event_source_arn":"arn:aws:kinesis:ap-northeast-1:111122223333:stream/test-stream","shard_id":"shardId-000000000000","value":230,"window_end":1637512020000,"window_start":1637511960000}
```

If you do not specify a config, an instant counter is used.  
Instant counters simply build counters from CLI options.  


For example, the following data is flowing to steam.
```json
{"request_id":1904,"time":"2021-12-01T11:19:54.24Z","user_id":1016}
{"request_id":1905,"time":"2021-12-01T11:19:54.3Z","user_id":1014}
{"request_id":1906,"time":"2021-12-01T11:19:54.36Z","user_id":1017}
{"request_id":1907,"time":"2021-12-01T11:19:54.42Z","user_id":1013}
{"request_id":1908,"time":"2021-12-01T11:19:54.48Z","user_id":1007}
...
```

If you want to get the estimated number of unique users per minute in the tumbling window:
```console
$ kinesis-data-counter -window 1m  -counter-type approx_count_distinct -counter-target-column user_id -stream test-stream
2021/11/22 01:24:36 [info] start get record from arn:aws:kinesis:ap-northeast-1:111122223333:stream/test-stream
{"counter_id":"__instant__","counter_type":"approx_count_distinct","event_source_arn":"arn:aws:kinesis:ap-northeast-1:111122223333:stream/test-stream","shard_id":"shardId-000000000000","value":30,"window_end":1637511900000,"window_start":1637511840000}
{"counter_id":"__instant__","counter_type":"approx_count_distinct","event_source_arn":"arn:aws:kinesis:ap-northeast-1:111122223333:stream/test-stream","shard_id":"shardId-000000000000","value":24,"window_end":1637511960000,"window_start":1637511900000}
{"counter_id":"__instant__","counter_type":"approx_count_distinct","event_source_arn":"arn:aws:kinesis:ap-northeast-1:111122223333:stream/test-stream","shard_id":"shardId-000000000000","value":22,"window_end":1637512020000,"window_start":1637511960000}
```

### as AWS Lambda function

`kinesis-data-counter` binary also runs as AWS Lambda function. 
kinesis-data-counter can be run as a bootstrap using a Lambda function.
When run as a Lambda function, expect to receive a KinesisTimeWindowEvent.

For details, refer to the TimeWindow item [here](https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html).

Sample Configure file as following:

```yaml
required_version: ">=0.0.0"

counters:
  - id: unique_user_count
    input_stream_arn: arn:aws:kinesis:*:*:stream/input-stream
    output_stream_arn: arn:aws:kinesis:ap-northeast-1:111122223333:stream/output-stream
    target_column: user_id
    counter_type: approx_count_distinct
    jq_expr: |
      {"time":.window_start, "name": "access_log.user_count", "value": .value}
```

Example Lambda functions configuration.

```json
{
  "FunctionName": "kinesis-data-counter",
  "Environment": {
    "Variables": {
      "KINESIS_DATA_COUNTER_CONFIG": "config.yaml",
    }
  },
  "Handler": "bootstrap",
  "MemorySize": 128,
  "Role": "arn:aws:iam::111122223333:role/lambda-function",
  "Runtime": "provided.al2",
  "Timeout": 300
}
```

It is possible to set multiple counters and count each input_source_arn.
It is also possible to set multiple counters for one input_source_arn.

**notice:**
- Always set the `--tumbling-window-in-seconds` parameter when doing create-event-source-mapping.
- It is recommended to set `aggregate_source_arn` when the number of shards of the kinesis stream that becomes input_source is 2 or more.


### `aggregate_source_arn` 

For example.

```yaml
required_version: ">=0.0.0"

counters:
  - id: unique_user_count
    input_stream_arn: arn:aws:kinesis:*:*:stream/input-stream
    aggregate_stream_arn: arn:aws:kinesis:ap-northeast-1:111122223333:stream/aggregate-stream
    output_stream_arn: arn:aws:firehose:ap-northeast-1:111122223333:deliverystream/output-to-s3
    target_column: user_id
    counter_type: approx_count_distinct
    jq_expr: |
      {"time":.window_start, "name": "access_log.user_count", "value": .value}
```

Lambda is Invoked per tumbling window and shard.  
If the input stream has more than one shard, it counts for each shard.  
For example, if the counter type is `approx_count_distinct`, it will be a unique count for each shard and will not be the number you want.  
If `aggregate_stream_arn` is set, after receiving the KinesisTimeWindowEvent from the input stream specified in` input_stream_arn`, the record of the intermediate calculation result is immediately put to PutRecord in the Kinesis Data Stream specified in `aggregate_stream_arn`.  
If you have set a Lambda function that receives the same KinesisTimeWindowEvent for Aggregate Stream, that Lambda function calculates the value of the counter that straddles the final Shard and puts it to Output Stream.  


### Counters

#### counter_type: `count`

A counter of type count simply counts.  
If `target_column` is` * `, count the amount of data flowing in the kinesis data stream.  
If any key is set in `target_column`, the JSON key is set and the number that does not appear null is counted.  
If an expression is set in `target_expr`, it evaluates the JSON record and counts it if it is not `nil` or `false`.  


#### counter_type: `approx_count_distinct`

Estimated unique count using HyperLogLog++.  
I think you can get a generally correct unique count.  

`target_column` is` * `cannot be set.  
If any key is set in `target_column`, the JSON key is set and the number that does not appear null is counted.  
If an expression is set in `target_expr`, it evaluates the JSON record and uniquely counts those that do not result in nil.  
SipHash is used as the column hashing algorithm. If you want to use something other than the default HashKey, specify `siphash_key_hex`  

### `jq_expr`

The final output can be processed using the jq expr.

## LICENSE

MIT
