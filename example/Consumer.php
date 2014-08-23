<?php
require 'autoloader.php';

$consumer = \Kafka\Consumer::getInstance('localhost:2181');
$group = 'testgroup1';
$consumer->setGroup($group);
$consumer->setFromOffset(true);
$consumer->setPartition('recom_page', 0);
$consumer->setMaxBytes(102400);
$result = $consumer->fetch();
foreach ($result as $topicName => $partition) {
    foreach ($partition as $partId => $messageSet) {
	var_dump($partition->getHighOffset());
        foreach ($messageSet as $message) {
            var_dump((string)$message);    
        }
	var_dump($partition->getMessageOffset());
    }
}
