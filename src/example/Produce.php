<?php
require 'autoloader.php';

$produce = \Kafka\Produce::getInstance('192.168.1.115', '2181');

$produce->setRequireAck(-1);
$produce->setMessages('test', 0, array('test1111111'));
$produce->setMessages('test6', 0, array('test1111111'));
$produce->setMessages('test6', 2, array('test1111111'));
$produce->setMessages('test6', 1, array('test111111111133'));
$result = $produce->send();
var_dump($result);
