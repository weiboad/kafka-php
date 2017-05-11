<?php
require '../vendor/autoload.php';
date_default_timezone_set('PRC');
use Monolog\Logger;
use Monolog\Handler\StdoutHandler;
// Create the logger
$logger = new Logger('my_logger');
// Now add some handlers
$logger->pushHandler(new StdoutHandler());

$config = \Kafka\ProducerConfig::getInstance();
$config->setMetadataRefreshIntervalMs(10000);
$config->setMetadataBrokerList('127.0.0.1:9292');
$config->setBrokerVersion('0.10.0.1');
$config->setRequiredAck(1);
$config->setIsAsyn(false);
$config->setProduceInterval(500);
$producer = new \Kafka\Producer(function() {
	return array(
		array(
			'topic' => 'test',
			'value' => 'test....message.',
			'key' => '',
		),
	);
});
$producer->setLogger($logger);
$producer->success(function($result) {
	var_dump($result);
});
$producer->error(function($errorCode, $context) {
	var_dump($errorCode);
});
$producer->send();
