<?php
require 'autoloader.php';

$zk = new \Kafka\ZooKeeper('hadoop11:2181', 3000);
$list = $zk->getConsumersPerTopic('testgroup');

var_dump($list);
//$zk = new \Zookeeper('hadoop11:2181');

//$path = '/consumers/testgroup/ids/2';
////$path = '/consumers/testgroup/ids';
//var_dump($zk->get($path));
