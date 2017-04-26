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
	'test'
);

$meta = new \Kafka\Protocol\Metadata('0.10.1.0');

$requestData = $meta->encode($data);

$socket = new \Kafka\SocketAsyn('127.0.0.1', '9192');
$socket->SetonReadable(function($data) use($meta) {
	$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
	var_dump($coodid);
});

$socket->connect();
$socket->write($requestData);
Amp\run(function () use ($socket, $requestData) {
});
