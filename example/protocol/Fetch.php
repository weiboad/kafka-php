<?php
require '../../vendor/autoload.php';
date_default_timezone_set('PRC');
use Monolog\Logger;
use Monolog\Handler\StdoutHandler;
// Create the logger
$logger = new Logger('my_logger');
// Now add some handlers
$logger->pushHandler(new StdoutHandler());

$data = json_decode('{"max_wait_time":100,"replica_id":-1,"min_bytes":"1000","data":[{"topic_name":"test","partitions":[{"partition_id":5,"offset":0,"max_bytes":2097152},{"partition_id":0,"offset":1855510,"max_bytes":2097152}]}]}', true);
var_dump($data);


$offset = new \Kafka\Protocol\Fetch('0.9.0.0');

$requestData = $offset->encode($data);

$socket = new \Kafka\SocketAsyn('10.75.26.24', '9192');

$socket->SetonReadable(function($data) use($offset) {
	$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
	var_dump($coodid);
	var_dump($offset->decode(substr($data, 4)));
});

$socket->connect();
$socket->write($requestData);
Amp\run(function () use ($socket, $requestData) {
});
