<?php
require '../vendor/autoload.php';

\Kafka\Protocol::init('1.0.0');
$options  = [
    'username' => 'nmred',
    'password' => '123456',
];
$provider = new \Kafka\Sasl\Plain($options);

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
$dataLen       = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $socket->selectRead(4));
$data          = $socket->selectRead($dataLen);
$correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
$result        = \Kafka\Protocol::decode(\Kafka\Protocol::PRODUCE_REQUEST, substr($data, 4));
var_dump($result);
