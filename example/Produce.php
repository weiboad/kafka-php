<?php
require 'autoloader.php';

while (1) {
    $part = mt_rand(0, 1);
    $produce = \Kafka\Produce::getInstance('127.0.0.1:2181', 3000);

	// get available partitions
	$partitions = $produce->getAvailablePartitions('test');
	var_dump($partitions);

	// send message
    $produce->setRequireAck(-1);
    $produce->setMessages('test', 0, array('test11111110099090'));
    $produce->setMessages('test', 1, array('test11111110099090'));
    $result = $produce->send();
    var_dump($result);
    usleep(10000);
}
