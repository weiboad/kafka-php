<?php
require '../../vendor/autoload.php';

$data = array(
	'required_ack' => 1,
	'timeout' => '1000',
	'data' => array(
		array(
			'topic_name' => 'test',
			'partitions' => array(
				array( 
					'partition_id' => 0,
					'messages' => array(
						array('key' => 'testkey', 'value' => 'test...'),
						'test...',
					),
				),
			),
		),
	),
);

$protocol = \Kafka\Protocol::init('0.9.1.0');
$requestData = \Kafka\Protocol::encode(\Kafka\Protocol::PRODUCE_REQUEST, $data);

$socket = new \Kafka\Socket('127.0.0.1', '9192');
$socket->SetonReadable(function($data) {
	$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
	$result = \Kafka\Protocol::decode(\Kafka\Protocol::PRODUCE_REQUEST, substr($data, 4));
	echo json_encode($result);
	Amp\stop();
});

$socket->connect();
$socket->write($requestData);
Amp\run(function () use ($socket, $requestData) {
});

