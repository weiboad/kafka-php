<?php
require 'autoloader.php';

$zk = new \Kafka\ZooKeeper('localhost', 2181);
$list = $zk->listBrokers();
var_dump($list);
