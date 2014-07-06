<?php
require 'autoloader.php';

$data = array(
    'test1',
);

$conn = new \Kafka\Socket('192.168.1.115', '9092');
$conn->connect();

$encoder = new \Kafka\Protocol\Encoder($conn);
$encoder->metadataRequest($data);

$decoder = new \Kafka\Protocol\Decoder($conn);
$result = $decoder->metaDataResponse();
var_dump($result);
