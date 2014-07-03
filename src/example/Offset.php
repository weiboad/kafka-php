<?php
require 'autoloader.php';

$data = array(
	'data' => array(
		array(
			'topic_name' => 'test',
			'partitions' => array(
				array(
					'partition_id' => 0,
                    'max_offset' => 1,
                    'time'  => microtime() 
				),
			),
		),
	),
);

$conn = new \Kafka\Socket('192.168.1.115', '9092');
$conn->connect();
$data = \Kafka\Protocol\Encoder::buildOffsetRequest($data);
var_dump(bin2hex($data));
$conn->write($data);
//var_dump(\Kafka\Protocol\Encoder::unpackInt64($conn->read(8))); // partition count

$dataLen = unpack('N', $conn->read(4));
$dataLen = $dataLen[1];
$data = $conn->read($dataLen);
var_dump(bin2hex($data));
$result = \Kafka\Protocol\Decoder::decodeOffsetResponse($data);
var_dump($result);
