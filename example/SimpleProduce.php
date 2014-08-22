<?php
require 'autoloader.php';

$msg = '/weibo/indexrightrecom?cuid=1738550761&ouid=1738550761&lang=zh-cn&gender=f&version=5&province=100&city=1000&ip=27.37.36.86&url=http%253A%252F%252Fweibo.com%252Fpls%252Fcommonapi%253Frequest_group%253D5'
. time();
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
						$msg,
					),
				),
			),
		),
	),
);
echo $msg;

$conn = new \Kafka\Socket('10.13.40.98', '9092');
$conn->connect();
$encoder = new \Kafka\Protocol\Encoder($conn);
$encoder->produceRequest($data);

$decoder = new \Kafka\Protocol\Decoder($conn);
$result = $decoder->produceResponse();
var_dump($result);
