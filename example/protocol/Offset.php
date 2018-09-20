<?php
require '../../vendor/autoload.php';

$data = [
    'replica_id' => -1,
    'data' => [
        [
            'topic_name' => 'test',
            'partitions' => [
                [
                    'partition_id' => 0,
                    'offset' => 12,
                    'time' => -2,
                ],
            ],
        ],
    ],
];

$protocol    = \Kafka\Protocol::init('0.10.1.0');
$requestData = \Kafka\Protocol::encode(\Kafka\Protocol::OFFSET_REQUEST, $data);

$socket = new \Kafka\connections\Socket('127.0.0.1', '9292');
$socket->setOnReadable(function ($data) {
    $coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
    $result = \Kafka\Protocol::decode(\Kafka\Protocol::OFFSET_REQUEST, substr($data, 4));
    echo json_encode($result);
    Amp\stop();
});

$socket->connect();
$socket->write($requestData);
Amp\run(function () use ($socket, $requestData) {
});
