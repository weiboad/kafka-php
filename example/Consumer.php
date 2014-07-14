<?php
require 'autoloader.php';

$consumer = \Kafka\Consumer::getInstance('localhost', '2181');
$group = 'testgroup';
$consumer->setGroup($group);
$consumer->setFromOffset(false);
$consumer->setPartition('page_recom', 0);
$consumer->setPartition('page_recom', 51);
$consumer->setPartition('page_recom', 20);
$consumer->setPartition('page_recom', 3);
$consumer->setPartition('page_recom', 4);
$result = $consumer->fetch();
foreach ($result as $topicName => $partition) {
    foreach ($partition as $partId => $messageSet) {
        foreach ($messageSet as $message) {
            var_dump((string)$message);    
        }
    }
}
