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
                    'offset' => 34,
					'max_bytes' => 1024,
				),
			),
		),
	),
);

$conn = new \Kafka\Socket('10.13.40.98', '9092');
$conn->connect();

$encoder = new \Kafka\Protocol\Encoder($conn);
$encoder->fetchRequest($data);

$decoder = new \Kafka\Protocol\Decoder($conn);
$topic = $decoder->fetchResponse();
foreach ($topic as $topic_name => $partition) {
	foreach ($partition as $partId => $messageSet) {
        var_dump($partition->getHighOffset());
		foreach ($messageSet as $message) {
			var_dump((string)$message);	
		}
	}
}
