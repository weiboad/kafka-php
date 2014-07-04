<?php
require 'autoloader.php';

$zk = new \Kafka\ZooKeeper('localhost', 2181);
$list = $zk->getPartitionState('test', 0);
var_dump($list);
