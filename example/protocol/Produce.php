<?php
require '../../vendor/autoload.php';

$data = [
    'required_ack' => 1,
    'timeout' => '1000',
    'data' => [
        [
            'topic_name' => 'test1',
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

$protocol    = \Kafka\Protocol::init('1.0.0');
$requestData = \Kafka\Protocol::encode(\Kafka\Protocol::PRODUCE_REQUEST, $data);

$socket = new \Kafka\connections\Socket('127.0.0.1', '9092');
$socket->setOnReadable(function ($data) {
    $coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
    $result = \Kafka\Protocol::decode(\Kafka\Protocol::PRODUCE_REQUEST, substr($data, 4));
    echo json_encode($result);
    Amp\Loop::stop();
});

$socket->connect();
$socket->write($requestData);
Amp\Loop::run(function () use ($socket, $requestData) {
});
