<?php
require '../../vendor/autoload.php';

$data = array(
	'test'
);

$protocol = \Kafka\Protocol::init('0.9.1.0');
$requestData = \Kafka\Protocol::encode(\Kafka\Protocol::METADATA_REQUEST, $data);
$socket = new \Kafka\Socket('10.13.4.159', '9192');
$socket->SetonReadable(function($data) {
	$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
	$result = \Kafka\Protocol::decode(\Kafka\Protocol::METADATA_REQUEST, substr($data, 4));
	echo json_encode($result);
	var_dump($result);
	Amp\stop();
});

$socket->connect();
$socket->write($requestData);
Amp\run(function () use ($socket, $requestData) {
});
