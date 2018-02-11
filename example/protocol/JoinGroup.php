<?php
declare(strict_types=1);

require '../../vendor/autoload.php';

use Kafka\Protocol;
use Kafka\Socket;

$data = [
    'group_id' => 'test',
    'session_timeout' => 6000,
    'rebalance_timeout' => 6000,
    'member_id' => '',
    'protocol_type' => 'testtype',
    'data' => [
        [
            'protocol_name' => 'group',
            'version' => 0,
            'subscription' => ['test'],
            'user_data' => '',
        ],
    ],
];

Protocol::init('0.9.1.0');
$requestData = Protocol::encode(Protocol::JOIN_GROUP_REQUEST, $data);
var_dump(bin2hex($requestData));

$socket = new Socket('127.0.0.1', '9192');
$socket->setOnReadable(function ($data): void {
    $coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
    $result = Protocol::decode(Protocol::JOIN_GROUP_REQUEST, substr($data, 4));
    echo bin2hex(substr($data, 4));
    echo json_encode($result);
    Amp\stop();
});

$socket->connect();
$socket->write($requestData);
Amp\run(function () use ($socket, $requestData): void {
});
