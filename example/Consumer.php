<?php
require 'autoloader.php';

$consumer = \Kafka\Consumer::getInstance('localhost', '2181');
$group = 'testgroup';
$consumer->setGroup($group);
$consumer->setFromOffset(false);
$consumer->setPartition('test', 0);
$result = $consumer->fetch();
foreach ($result as $topicName => $partition) {
    foreach ($partition as $partId => $messageSet) {
        foreach ($messageSet as $message) {
            var_dump((string)$message);    
        }
    }
}
