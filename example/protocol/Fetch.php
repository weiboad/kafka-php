<?php
require '../../vendor/autoload.php';

$data = [
    'max_wait_time' => 1000,
    'replica_id' => -1,
    'min_bytes' => 1000,
    'data' => [
        [
            'topic_name' => 'test',
            'partitions' => [
                [
                    'partition_id' => 0,
                    'offset' => 45,
                    'max_bytes' => 1024,
                ]
            ],
        ],
    ],
];

\Kafka\Protocol::init('0.9.1.0');
$requestData = \Kafka\Protocol::encode(\Kafka\Protocol::FETCH_REQUEST, $data);

$socket = new \Kafka\Socket('127.0.0.1', '9192');
$socket->setOnReadable(function ($data) {
    $coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
    $result = \Kafka\Protocol::decode(\Kafka\Protocol::FETCH_REQUEST, substr($data, 4));
    echo json_encode($result);
    Amp\stop();
});

$socket->connect();
$socket->write($requestData);
Amp\run(function () use ($socket, $requestData) {
});
