<?php
declare(strict_types=1);

require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use Amp\Loop;
use Kafka\Producer;
use Kafka\ProducerConfig;
use Monolog\Handler\StdoutHandler;
use Monolog\Logger;

// Create the logger
$logger = new Logger('my_logger');
// Now add some handlers
$logger->pushHandler(new StdoutHandler());

$config = ProducerConfig::getInstance();
$config->setMetadataRefreshIntervalMs(1000);
$config->setMetadataBrokerList('127.0.0.1:9092');
$config->setBrokerVersion('1.0.0');
$config->setRequiredAck(1);
$config->setIsAsyn(true);
$config->setProduceInterval(500);

class Message
{
    /**
     * @var string[]
     */
    private $message;

    /**
     * @return string[]
     */
    public function getMessage(): array
    {
        return $this->message;
    }

    /**
     * @param string[] $message
     */
    public function setMessage(array $message): void
    {
        $this->message = $message;
    }
}

// control message send interval time
$message = new Message;
Loop::repeat(3000, function () use ($message): void {
    $message->setMessage([
        [
            'topic' => 'test',
            'value' => 'test....message.' . time(),
            'key' => '',
        ],
    ]);
});

$producer = new Producer(function () use ($message) {
    $tmp = $message->getMessage();
    $message->setMessage([]);
    return $tmp;
});
$producer->setLogger($logger);
$producer->success(function ($result): void {
    var_dump($result);
});
$producer->error(function ($errorCode, $context): void {
    var_dump($errorCode);
});
$producer->send();
