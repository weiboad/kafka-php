<?php
declare(strict_types=1);

require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use Kafka\Producer;
use Kafka\ProducerConfig;
use Monolog\Handler\StdoutHandler;
use Monolog\Logger;

// Create the logger
$logger = new Logger('my_logger');
// Now add some handlers
$logger->pushHandler(new StdoutHandler());

$config = ProducerConfig::getInstance();
$config->setMetadataRefreshIntervalMs(10000);
$config->setMetadataBrokerList('127.0.0.1:9093');
$config->setBrokerVersion('0.10.2.1');
$config->setRequiredAck(1);
$config->setIsAsyn(false);
$config->setProduceInterval(500);

// if use ssl connect
//$config->setSslLocalCert('/home/vagrant/code/kafka-php/ca-cert');
//$config->setSslLocalPk('/home/vagrant/code/kafka-php/ca-key');
//$config->setSslEnable(true);
//$config->setSslPassphrase('123456');
//$config->setSslPeerName('nmred');

$producer = new Producer();
$producer->setLogger($logger);

for ($i = 0; $i < 100; $i++) {
    $result = $producer->send([
        [
            'topic' => 'test',
            'value' => 'test1....message.',
            'key' => '',
        ],
    ]);
    var_dump($result);
}
