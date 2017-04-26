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

$data = json_decode('{"group_id":"test","generation_id":1,"member_id":"kafka-php-acc0fdd6-8e03-4041-a8ee-92603c75c51d","data":{"test":{"partitions":{"8":{"partition":8,"offset":0},"3":{"partition":3,"offset":3},"2":{"partition":2,"offset":23},"7":{"partition":7,"offset":0},"5":{"partition":5,"offset":0},"0":{"partition":0,"offset":1850823},"4":{"partition":4,"offset":4},"9":{"partition":9,"offset":2},"1":{"partition":1,"offset":1697423},"6":{"partition":6,"offset":1}},"topic_name":"test"}}}', true);

$group = new \Kafka\Protocol\CommitOffset('0.9.1.0');

$requestData = $group->encode($data);
var_dump(\bin2hex($requestData));

//$socket = new \Kafka\SocketAsyn('10.13.4.159', '9192');
//$socket->SetonReadable(function($data) use($group) {
//	$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
//	var_dump($coodid);
//	var_dump($group->decode(substr($data, 4)));
//	var_dump($dataLen);
//});
//
//$socket->connect();
//$socket->write($requestData);
//Amp\run(function () use ($socket, $requestData) {
//});
