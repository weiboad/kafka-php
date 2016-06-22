<?php
require 'autoloader.php';

$zookeeperList = getenv('ZOOKEEPER_LIST');
$zk = new \Kafka\ZooKeeper($zookeeperList, 3000);
$groupId = 'testgroup';
$consumerId = 2;
$topics = array(
	'recom_page',				
);

function callback() {
	echo "Register";
}
$path = '/consumers/testgroup/ids';
////$path = '/consumers/testgroup/ids';
//var_dump($zk->get($path));
$zk->watch($path, 'callback');
//$zk->registerConsumer($groupId, $consumerId, $topics);
$zk->registerConsumer($groupId, $consumerId, $topics);
$list = $zk->getConsumersPerTopic('testgroup');

var_dump($list);
//$zk = new \Zookeeper('hadoop11:2181');



while (1) {
 sleep(1);
}
