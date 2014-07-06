<?php
require 'autoloader.php';

$consumer = \Kafka\Consumer::getInstance('192.168.1.115', '2181');

$consumer->setGroup('testgroup');
$consumer->setPartition('test', 0);
$consumer->setPartition('test6', 2, 10);
$result = $consumer->fetch();
foreach ($result as $topicName => $topic) {
    foreach ($topic as $partId => $partition) {
        foreach ($partition as $message) {
            var_dump((string)$message);    
        }
    }
}
