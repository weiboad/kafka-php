<?php
require 'autoloader.php';

$data = array(
	'required_ack' => 1,
	'timeout' => 1000,
	'data' => array(
		array(
			'topic_name' => 'test',
			'partitions' => array(
				array(
					'partition_id' => 0,
					'messages' => array(
						'32321`1```````````',
						'message2',
					),
				),
			),
		),
		array(
			'topic_name' => 'test1',
			'partitions' => array(
				array(
					'partition_id' => 0,
					'messages' => array(
						'32321`1```````````',
						'message2',
					),
				),
				array(
					'partition_id' => 1,
					'messages' => array(
						'32321`1```````````',
						'message2',
					),
				),
			),
		),
	),
);

$conn = new \Kafka\Socket('localhost', '9092');
$conn->connect();
$data = \Kafka\Protocol\Encoder::buildProduceRequest($data);
$conn->write($data);
//var_dump(\Kafka\Protocol\Encoder::unpackInt64($conn->read(8))); // partition count

$dataLen = unpack('N', $conn->read(4));
$dataLen = $dataLen[1];
$data = $conn->read($dataLen);
$initoffset = 4;
$topicCount = unpack('N', substr($data, $initoffset, 4));
$initoffset += 4;
$topicCount = $topicCount[1];
for ($i = 0; $i < $topicCount; $i++) {
	$topicLen = unpack('n', substr($data, $initoffset, 2));	
	$topicLen = $topicLen[1];		
	$initoffset += 2;
	$topic_name = substr($data, $initoffset, $topicLen);
	$initoffset += $topicLen;
	$partitionCount = unpack('N', substr($data, $initoffset, 4));
	$partitionCount = $partitionCount[1];
	$initoffset += 4;
	for ($j = 0; $j < $partitionCount; $j++) {
		$info = unpack('Nn', substr($data, $initoffset, 6));
		$initoffset += 6;
		$offset = \Kafka\Protocol\Encoder::unpackInt64(substr($data, $initoffset, 8));
		$initoffset += 8;
	var_dump($offset);
	}
	var_dump($topic_name);
}
