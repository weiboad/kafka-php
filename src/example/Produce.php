<?php
require 'autoloader.php';

$produce = \Kafka\Produce::getInstance('localhost', '2181');

$produce->setRequireAck(-1);
$produce->setMessages('test', 0, array('test1111111'));
$produce->setMessages('test1', 0, array('test1111111'));
$produce->setMessages('test1', 1, array('test111111111133'));
$result = $produce->send();
var_dump($result);
