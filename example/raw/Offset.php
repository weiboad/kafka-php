<?php
require '../../vendor/autoload.php';

date_default_timezone_set('PRC');

use Monolog\Logger;
use Monolog\Handler\StdoutHandler;

// Create the logger
$logger = new Logger('my_logger');
// Now add some handlers
$logger->pushHandler(new StdoutHandler());

$mutilTest = array(
	'0.8.2.1' => array('127.0.0.1', 9092),
	'0.9.0.0' => array('127.0.0.1', 9192),
	'0.10.1.0' => array('127.0.0.1', 9292),
);
$data = array(
	'replica_id' => -1,
	'data' => array(
		array(
			'topic_name' => 'test',
			'partitions' => array(
				array( 
					'partition_id' => 0,
					'offset' => 100,
					'time' => -1,
				),
			),
		),
	),
);

foreach ($mutilTest as $version => $hostInfo) {
	echo 'Start test version:' . $version . PHP_EOL;
	$conn = new \Kafka\Socket($hostInfo[0], $hostInfo[1]);
	$conn->connect();

	$encoder = new \Kafka\Protocol\Encoder($conn, $version);
	$encoder->setLogger($logger);
	$ret = $encoder->offsetRequest($data);

	$decoder = new \Kafka\Protocol\Decoder($conn, $version);
	$result = $decoder->offsetResponse();
	var_dump($result);
}
