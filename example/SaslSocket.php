<?php
require '../vendor/autoload.php';

use \Kafka\Sasl\Scram;

\Kafka\Protocol::init('1.0.0');
//$provider = new \Kafka\Sasl\Plain('nmred', '123456');
//$provider = new \Kafka\Sasl\Gssapi('/etc/security/keytabs/kafkaclient.keytab', 'kafka/node1@NMREDKAFKA.COM');
$provider = new Scram('alice', 'alice-secret', Scram::SCRAM_SHA_256);

$socket = new \Kafka\SocketSync('127.0.0.1', '9092');
$socket->setSaslProvider($provider);
$socket->connect();

$data = [
    'required_ack' => 1,
    'timeout' => '1000',
    'data' => [
        [
            'topic_name' => 'test',
            'partitions' => [
                [
                    'partition_id' => 0,
                    'messages' => [
                        ['key' => 'testkey', 'value' => 'test...'],
                        'test...',
                    ],
                ],
            ],
        ],
    ],
];


$requestData = \Kafka\Protocol::encode(\Kafka\Protocol::PRODUCE_REQUEST, $data);
$socket->write($requestData);
$dataLen       = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $socket->readBlocking(4));
$data          = $socket->readBlocking($dataLen);
$correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
$result        = \Kafka\Protocol::decode(\Kafka\Protocol::PRODUCE_REQUEST, substr($data, 4));
var_dump($result);
