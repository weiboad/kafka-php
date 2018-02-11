<?php
declare(strict_types=1);

require '../../vendor/autoload.php';

use Kafka\Protocol;
use Kafka\Socket;

$data = 'PLAIN';
Protocol::init('1.0.0');
$requestData = Protocol::encode(Protocol::SASL_HAND_SHAKE_REQUEST, $data);

$socket = new Socket('127.0.0.1', '9092');
$socket->setOnReadable(function ($data): void {
    $coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
    $result = Protocol::decode(Protocol::SASL_HAND_SHAKE_REQUEST, substr($data, 4));
    echo json_encode($result);
    var_dump($result);
    Amp\Loop::stop();
});

$socket->connect();
$socket->write($requestData);
Amp\Loop::run(function () use ($socket, $requestData): void {
});
