<?php
require 'autoloader.php';

$produce = \Kafka\Produce::getInstance('localhost', '2181');

$produce->setRequireAck(-1);
$produce->setMessages('test', 0, array('test11111110099090'));
//$produce->setMessages('recom_mobile', 0, array('test11111111112332'));
$produce->setMessages('recom_mobile', 2, array('test1111111'));
$produce->setMessages('recom_mobile', 1, array('test111111111133'));
$result = $produce->send();
var_dump($result);
