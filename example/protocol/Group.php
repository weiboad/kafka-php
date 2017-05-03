<?php
require '../../vendor/autoload.php';

$data = array(
	'group_id' => 'test',
);

$protocol = \Kafka\Protocol::init('0.9.1.0');
$requestData = \Kafka\Protocol::encode(\Kafka\Protocol::GROUP_COORDINATOR_REQUEST, $data);
var_dump(\bin2hex($requestData));

$socket = new \Kafka\Socket('10.13.4.159', '9192');
$socket->SetonReadable(function($data) use($group) {
	$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
	var_dump(\bin2hex(substr($data, 4)));
	$result = \Kafka\Protocol::decode(\Kafka\Protocol::GROUP_COORDINATOR_REQUEST, substr($data, 4));
	echo json_encode($result);
	Amp\stop();
});

$socket->connect();
$socket->write($requestData);
Amp\run(function () use ($socket, $requestData) {
});
