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
//	'0.8.2.1' => array('127.0.0.1', 9092),
	'0.9.0.0' => array('127.0.0.1', 9192),
	//'0.10.1.0' => array('127.0.0.1', 9292),
);
$join = array(
	'group_id' => 'test1',
	'session_timeout' => 6001,
	'rebalance_timeout' => 60000,
	'member_id' => '',
	'data' => array(
		array(
			'protocol_name' => 'group',
			'version' => 0,
			'subscription' => array('test', 'test22'),
			'user_data' => '111',
		),
	),
);

$test = array(0, 1);

//00057465
//	737431000927c000000008636f6e73756d657200000001000567726f7570000000100000000000010004746573740000000000000049000b00000000000000096b61666b612d70687000057465737431000927c000000008636f6e73756d657200000001000567726f75700000001000000000000100047465737400000000

foreach ($mutilTest as $version => $hostInfo) {
	echo 'Start test version:' . $version . PHP_EOL;
	$conn = new \Kafka\Socket($hostInfo[0], $hostInfo[1], 30);
	$conn->connect();

	$encoder = new \Kafka\Protocol\Encoder($conn, $version);
	$encoder->setLogger($logger);
	$ret = $encoder->joinGroupRequest($join);

	$decoder = new \Kafka\Protocol\Decoder($conn, $version);
	$result = $decoder->joinGroupResponse();

	if ($result['errorCode'] != 0)	{
		echo 'Join group fail, errorCode:' . $result['errorCode'] . PHP_EOL;
		continue;
	}

	$sync = array(
		'group_id' => 'test1',
		'generation_id' => $result['generationId'],
		'member_id' => $result['memberId'],
		'data' => array()
	);
	if ($result['leaderId'] == $result['memberId']) { // leader
		$count = count($result['members']);

		$partitions = array();
		foreach ($test as $key => $partition) {
			$member = $key % $count;
			$partitions[$member][] = $partition;
		}
		foreach ($result['members'] as $key => $memberInfo) {
			$item = array(
				'version' => 0,
				'member_id' => $memberInfo['memberId'],
				'assignments' => array(
					array(
						'topic_name' => 'test',
						'partitions' => $partitions[$key],
					)
				)
			);
			$sync['data'][] = $item;
		}
	}
	var_dump($result);
	$encoder = new \Kafka\Protocol\Encoder($conn, $version);
	$encoder->setLogger($logger);
	$ret = $encoder->syncGroupRequest($sync);

	$decoder = new \Kafka\Protocol\Decoder($conn, $version);
	$result = $decoder->syncGroupResponse();
	var_dump($result);
}
