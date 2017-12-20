<?php
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use Kafka\Producer;
use Psr\Log\LoggerInterface;
use Monolog\Logger;
use Monolog\Handler\StdoutHandler;

$container = Producer::getContainer([
    \Kafka\Contracts\SocketInterface::class => \DI\object(\Kafka\Socket\SocketUnblocking::class),
    // configure logger
    LoggerInterface::class => function () {
        // Create the logger
        $logger = new Logger('my_logger');
        // Now add some handlers
        $logger->pushHandler(new StdoutHandler());
        return $logger;
    },
    // configure broker
    \Kafka\Contracts\Config\Broker::class => function () {
        $config = new \Kafka\Config\Broker();
        $config->setMetadataBrokerList(getenv('KAFKA_BROKERS'));
        return $config;
    },
    // configure sasl
    //\Kafka\Contracts\Config\Sasl::class => function () {
    //    $config = new \Kafka\Config\Sasl();
    //    $config->setUsername('nmred');
    //    $config->setPassword('123456');
    //    return $config;
    //}
]);

$messagesSent = false;

$stop = new \Kafka\StopStrategy\Callback(
    function () use (&$messagesSent): bool {
            return $messagesSent;
    },
    10
);


$producer = $container->make(Producer::class, ['producer' => function () {
    return [
        [
            'topic' => 'test',
            'value' => 'test....message.',
            'key' => '',
        ],
    ];
},  'stopStrategy' => $stop]);

$producer->success(function ($result) use (&$messagesSent) {
    $messagesSent = true;
    var_dump($result);
});
$producer->error(function ($errorCode) {
    var_dump($errorCode);
});
$producer->send(true);
