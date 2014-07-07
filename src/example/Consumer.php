<?php
require 'autoloader.php';

$consumer = \Kafka\Consumer::getInstance('localhost', '2181');
$group = 'testgroup';
$consumer->setGroup($group);
$consumer->setPartition('test', 0);
$consumer->setPartition('recom_mobile', 0);
$client = $consumer->getClient();
//$consumer->setPartition('test6', 2, 10);
$result = $consumer->fetch();
foreach ($result as $topicName => $partition) {
    foreach ($partition as $partId => $messageSet) {
		$offset = $partition->getHighOffset();
		//$offsetObject = new  \Kafka\Offset($client, $group, $topicName, $partId);
		//$offsetObject->setOffset($offset);
        foreach ($messageSet as $message) {
            var_dump((string)$message);    
        }
    }
}
