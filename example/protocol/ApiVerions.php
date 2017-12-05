<?php
require '../../vendor/autoload.php';

$data = 'PLAIN';
$protocol    = \Kafka\Protocol::init('1.0.0');
$requestData = \Kafka\Protocol::encode(\Kafka\Protocol::API_VERSIONS_REQUEST, $data);

$socket = new \Kafka\Socket('127.0.0.1', '9092');
$socket->setOnReadable(function ($data) {
    $coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
    $result = \Kafka\Protocol::decode(\Kafka\Protocol::API_VERSIONS_REQUEST, substr($data, 4));
    echo json_encode($result);
    Amp\Loop::stop();
});

$socket->connect();
$socket->write($requestData);
Amp\Loop::run(function () use ($socket, $requestData) {
});
