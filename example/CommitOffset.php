<?php
require 'autoloader.php';

$data = array(
    'group_id' => 'testgroup1',
	'data' => array(
		array(
			'topic_name' => 'test6',
			'partitions' => array(
				array(
                    'partition_id' => 2,
					'offset' => 2,
				),
			),
		),
	),
);

$conn = new \Kafka\Socket('hadoop11', '9092');
$conn->connect();

$encoder = new \Kafka\Protocol\Encoder($conn);
$encoder->commitOffsetRequest($data);

$decoder = new \Kafka\Protocol\Decoder($conn);
$result = $decoder->commitOffsetResponse();
var_dump($result);
