<?php
require '../../vendor/autoload.php';
date_default_timezone_set('PRC');
use Monolog\Logger;
use Monolog\Handler\StdoutHandler;
// Create the logger
$logger = new Logger('my_logger');
// Now add some handlers
$logger->pushHandler(new StdoutHandler());

$data = array(
	'group_id' => 'test',
	'data' => array(
		array(
			'topic_name' => 'test',
			'partitions' => array(1,2)
		)
	),
);

var_dump(json_encode($data));
$offset = new \Kafka\Protocol\FetchOffset('0.10.1.0');

$requestData = $offset->encode($data);

$socket = new \Kafka\SocketAsyn('10.13.4.159', '9192');
$socket->SetonReadable(function($data) use($offset) {
	$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
	var_dump($coodid);
	var_dump($offset->decode(substr($data, 4)));
	var_dump($dataLen);
});

$socket->connect();
$socket->write($requestData);
Amp\run(function () use ($socket, $requestData) {
});
