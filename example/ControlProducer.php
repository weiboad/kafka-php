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
$config->setMetadataRefreshIntervalMs(1000);
$config->setMetadataBrokerList('127.0.0.1:9092');
$config->setBrokerVersion('1.0.0');
$config->setRequiredAck(1);
$config->setIsAsyn(true);
$config->setProduceInterval(500);

class Message
{
    private $message;

    public function getMessage()
    {
        return $this->message;
    }
    public function setMessage($message)
    {
        $this->message = $message;
    }
}

// control message send interval time
$message = new Message;
\Amp\Loop::repeat(3000, function () use ($message) {
    $message->setMessage([
        [
            'topic' => 'test',
            'value' => 'test....message.' . time(),
            'key' => '',
        ],
    ]);
});

$producer = new \Kafka\Producer(function () use ($message) {
    $tmp = $message->getMessage();
    $message->setMessage([]);
    return $tmp;
});
$producer->setLogger($logger);
$producer->success(function ($result) {
    var_dump($result);
});
$producer->error(function ($errorCode, $context) {
    var_dump($errorCode);
});
$producer->send();
