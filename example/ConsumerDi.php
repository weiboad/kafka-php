<?php
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use Kafka\Consumer;
use Psr\Log\LoggerInterface;
use Monolog\Logger;
use Monolog\Handler\StdoutHandler;

$container = Consumer::getContainer([
	\Kafka\Contracts\SocketInterface::class => \DI\object(\Kafka\Socket\SocketUnblocking::class),
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
	},
	\Kafka\Contracts\Config\Consumer::class => function() {
		$config = new \Kafka\Config\Consumer();
		$config->setTopics(['test']);
		$config->setGroupId('test1');
		return $config;
	}
]);

$consumer  = $container->make(Consumer::class);
$consumer->start(function($topic, $part, $message) {
	var_dump($message);
});
