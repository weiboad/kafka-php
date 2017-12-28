<?php
require '../../vendor/autoload.php';
use Kafka\Protocol;
use Kafka\Socket;

$data = [
    'group_id' => 'test',
    'data' => [
        [
            'topic_name' => 'test',
            'partitions' => [0],
        ],
    ],
];

Protocol::init('0.9.1.0');
$requestData = \Kafka\Protocol::encode(\Kafka\Protocol::OFFSET_FETCH_REQUEST, $data);
var_dump(bin2hex($requestData));

$socket = new Socket('127.0.0.1', '9192');
$socket->setOnReadable(function ($data): void {
    $coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
    $result = \Kafka\Protocol::decode(\Kafka\Protocol::OFFSET_FETCH_REQUEST, substr($data, 4));
    echo bin2hex(substr($data, 4));
    echo json_encode($result);
    Amp\stop();
});

$socket->connect();
$socket->write($requestData);
Amp\run(function () use ($socket, $requestData): void {
});
