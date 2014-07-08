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
                    'time'  => -1 
				),
			),
		),
	),
);

$conn = new \Kafka\Socket('localhost', '9092');
$conn->connect();

$encoder = new \Kafka\Protocol\Encoder($conn);
$encoder->offsetRequest($data);

$decoder = new \Kafka\Protocol\Decoder($conn);
$result = $decoder->offsetResponse();
var_dump($result);
