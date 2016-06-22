<?php
require 'autoloader.php';

class LogWriter {
	public function log($message, $level) {
		echo $message, PHP_EOL;
	}
}

$log = new LogWriter;
\Kafka\Log::setLog($log);

$zookeeperList = getenv('ZOOKEEPER_LIST'); 
$consumer = \Kafka\Consumer::getInstance($zookeeperList);
$group = 'testgroup1';
$consumer->setGroup($group);
$consumer->setFromOffset(true);
//$consumer->setPartition('recom_page', 0);
$consumer->setTopic('recom_page');
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
