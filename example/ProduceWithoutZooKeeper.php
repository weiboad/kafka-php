<?php
require 'autoloader.php';

while (1) {
	$hostList = getenv('KAFKA_LIST_V0_8');
    $produce = \Kafka\Produce::getInstance(null, null, $hostList);

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
