<?php

require '../vendor/autoload.php';

$config = \Kafka\ConsumerConfig::getInstance();
var_dump($config->setGroupId('dsds'));
var_dump($config->getGroupId());
