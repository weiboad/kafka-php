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
                    'offset' => 444,
				),
			),
		),
	),
);

$conn = new \Kafka\Socket('localhost', '9092');
$conn->connect();
$data = \Kafka\Protocol\Encoder::buildFetchRequest($data);
$conn->write($data);
$conn->read(8);
//var_dump(\Kafka\Protocol\Encoder::unpackInt64($conn->read(8))); // partition count

//$dataLen = unpack('N', $conn->read(4));
//$dataLen = $dataLen[1];
//$data = $conn->read($dataLen);
//var_dump(bin2hex($data));
$topic = new \Kafka\Protocol\Fetch\Topic($conn);
foreach ($topic as $topic_name => $partition) {
	foreach ($partition as $partId => $messageSet) {
		foreach ($messageSet as $message) {
			var_dump((string)$message);	
		}
	}
}
//$result = \Kafka\Protocol\Decoder::decodeProduceResponse($data);
//var_dump($result);
