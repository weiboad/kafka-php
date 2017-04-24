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
);

$group = new \Kafka\Protocol\GroupCoordinator('0.10.1.0');

$requestData = $group->encode($data);

$socket = new \Kafka\Broker\Socket('127.0.0.1', '9292');
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
