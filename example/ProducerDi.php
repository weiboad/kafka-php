<?php
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use Kafka\Producer;
use Psr\Log\LoggerInterface;
use Monolog\Logger;
use Monolog\Handler\StdoutHandler;

$container = Producer::getContainer([
	// configure logger
	LoggerInterface::class => function() {
		// Create the logger
		$logger = new Logger('my_logger');
		// Now add some handlers
		$logger->pushHandler(new StdoutHandler());
		return $logger;
	},
	// configure broker
	\Kafka\Contracts\Config\Broker::class => function() {
		$config = new \Kafka\Config\Broker();
		$config->setMetadataBrokerList('127.0.0.1:9092');
		return $config;
	},
	// configure sasl
	\Kafka\Contracts\Config\Sasl::class => function() {
		$config = new \Kafka\Config\Sasl();
		$config->setUsername('nmred');
		$config->setPassword('123456');
		return $config;
	}
]);

$saslConfig = $container->get(Kafka\Contracts\Config\Sasl::class);
$producer  = $container->make(Producer::class);
$ret = $producer->send(
	[
       [
           'topic' => 'test',
           'value' => 'test....message.',
           'key' => '',
       ],
   ]
);
var_dump($ret);
