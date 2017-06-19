Kafka-php Configuration
==================

| Property	| C/P	| Range	| Default | Desc |
| --  | -- | -- | -- | -- |
| clientId | C/P |  | kafka-php | This is a user supplied identifier for the client application | 
| brokerVersion | C/P | 大于 0.8.0 | 0.10.1.0 | User supplied broker version |
| metadataBrokerList | C/P | | | Kafka Broker server list |
| messageMaxBytes | C/P | 1000 .. 1000000000 | 1000000 | Maximum transmit message size. |
| metadataRequestTimeoutMs | C/P | 10 .. 900000 | 60000 | Non-topic request timeout in milliseconds. This is for metadata requests, etc. |
| metadataRefreshIntervalMs | C/P | 10 .. 3600000  | 300000 | Topic metadata refresh interval in milliseconds. The metadata is automatically refreshed on error and connect. Use -1 to disable the intervalled refresh.  |
| metadataMaxAgeMs | C/P | 1 .. 86400000 | -1 | Metadata cache max age. Defaults to metadata.refresh.interval.ms * 3 |
| groupId | C |  | |  Client group id string. All clients sharing the same group.id belong to the same group. |
| sessionTimeout | C | 1 .. 3600000 | 30000 | Client group session and failure detection timeout.  |
| rebalanceTimeout | C | 1 .. 3600000 | 30000 | rebalance join wait timeout |
| topics | C | | |  Want consumer topics | 
| offsetReset | C | latest,earliest | latest | Action to take when there is no initial offset in offset store or the desired offset is out of range |
| maxBytes | C |  | 65536 | Maximum bytes to fetch. |
| maxWaitTime | C |  | 100 | Maximum time in ms to wait for the response |
| requiredAck | P | -1 .. 1000 | 1 | This field indicates how many acknowledgements the leader broker must receive from ISR brokers before responding to the request: 0=Broker does not send any response/ack to client, 1=Only the leader broker will need to ack the message, -1 or all=broker will block until message is committed by all in sync replicas (ISRs) or broker\'s in.sync.replicas setting before sending response.  |
| timeout | P | 1 .. 900000 | 5000 | Producer request timeout |
| isAsyn | P | true, false | false | Whether to use asynchronous production messages |
| requestTimeout | P | 1 .. 900000 | 6000 |  The total timeout of the production message, which must be greater than the timeout config parameter |
| produceInterval | P | 1 .. 900000 | 100 | The time interval at which requests for production messages are executed when the message is produced asynchronously |

#### Note

All of the above parameters are set by setXxx, for example, to set the `clientId` parameter

```php
<?php

$config = \Kafka\ConsumerConfig::getInstance();
$config->setClientId('test');
```

Whether it is a consumer module or a production module, if the parameter settings do not match the rules will throw `\Kafka\Exception\Config` exception
