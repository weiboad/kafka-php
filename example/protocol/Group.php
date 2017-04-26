<?php
require '../../vendor/autoload.php';

$data = array(
	'group_id' => 'test',
);

$group = new \Kafka\Protocol\GroupCoordinator('0.9.1.0');

$requestData = $group->encode($data);

$socket = new \Kafka\SocketAsyn('10.13.4.159', '9192');
$socket->SetonReadable(function($data) use($group) {
	$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
	var_dump($coodid);
	var_dump($group->decode(substr($data, 4)));
	var_dump($dataLen);
});

$socket->connect();
$socket->write($requestData);
Amp\run(function () use ($socket, $requestData) {
});
