<?php
declare(strict_types=1);

require '../../vendor/autoload.php';

use Kafka\Protocol;
use Kafka\Socket;

$data = ['group_id' => 'test'];

Protocol::init('0.9.1.0');
$requestData = Protocol::encode(Protocol::GROUP_COORDINATOR_REQUEST, $data);

$socket = new Socket('127.0.0.1', '9092');
$socket->setOnReadable(function ($data): void {
    $coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
    $result = Protocol::decode(Protocol::GROUP_COORDINATOR_REQUEST, substr($data, 4));
    echo json_encode($result);
    Amp\Loop::stop();
});

$socket->connect();
$socket->write($requestData);
Amp\Loop::run(function () use ($socket, $requestData): void {
});
