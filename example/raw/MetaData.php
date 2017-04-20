<?php
require '../../vendor/autoload.php';

$mutilTest = array(
	'0.8.2.1' => array('127.0.0.1', 9092),
	'0.9.0.0' => array('10.13.4.159', 9192),
	'0.10.1.0' => array('127.0.0.1', 9292),
);
$data = array(
    'test',
);

foreach ($mutilTest as $version => $hostInfo) {
	echo 'Start test version:' . $version . PHP_EOL;
	$conn = new \Kafka\Socket($hostInfo[0], $hostInfo[1]);
	$conn->connect();

	$encoder = new \Kafka\Protocol\Encoder($conn, $version);
	$encoder->metadataRequest($data);

	$decoder = new \Kafka\Protocol\Decoder($conn, $version);
	$result = $decoder->metadataResponse();
	var_dump($result);
}
