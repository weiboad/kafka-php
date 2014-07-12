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
			'topic_name' => 'test6',
			'partitions' => array(
				array(
					'partition_id' => 2,
					'messages' => array(
						'32321`1```````````',
						'message2',
					),
				),
				array(
					'partition_id' => 5,
					'messages' => array(
						'9932321`1```````````',
						'message2',
					),
				),
			),
		),
	),
);

$conn = new \Kafka\Socket('192.168.1.115', '9092');
$conn->connect();
$encoder = new \Kafka\Protocol\Encoder($conn);
$encoder->produceRequest($data);

$decoder = new \Kafka\Protocol\Decoder($conn);
$result = $decoder->produceResponse();
var_dump($result);
