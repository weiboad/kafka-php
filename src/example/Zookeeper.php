<?php
require 'autoloader.php';

$zk = new \Kafka\ZooKeeper('localhost', 2181);
$list = $zk->registerConsumer('testgroup', 1, array('test'));

$zk = new \Zookeeper('localhost:2181');

$path = '/consumers/testgroup/owners/test/0';
$path = '/consumers/testgroup/ids/1';
var_dump($zk->get($path));
