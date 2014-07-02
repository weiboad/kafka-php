<?php
require 'autoloader.php';

$data = array(
    'test1',
);

$conn = new \Kafka\Socket('192.168.1.115', '9092');
$conn->connect();
$data = \Kafka\Protocol\Encoder::buildMetadataRequest($data);
var_dump(bin2hex($data));
$conn->write($data);
//var_dump(\Kafka\Protocol\Encoder::unpackInt64($conn->read(8))); // partition count

$dataLen = unpack('N', $conn->read(4));
$dataLen = $dataLen[1];
$data = $conn->read($dataLen);
var_dump(bin2hex($data));
$result = \Kafka\Protocol\Decoder::decodeMetaDataResponse($data);
var_dump($result);
