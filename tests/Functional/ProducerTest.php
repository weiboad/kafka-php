<?php
declare(strict_types=1);

namespace KafkaTest\Functional;

use Kafka\StopStrategy\Callback;
use Kafka\Contracts\Config\Broker;

abstract class ProducerTest extends \PHPUnit\Framework\TestCase
{
    private const MESSAGES_TO_SEND = 30;

    /**
     * @var string
     */
    private $version;

    /**
     * @var string
     */
    private $brokers;

    /**
     * @var string
     */
    private $topic;

    protected $container;

    /**
     * @before
     */
    public function prepareEnvironment(): void
    {
        $this->version = getenv('KAFKA_VERSION');
        $this->brokers = getenv('KAFKA_BROKERS');
        $this->topic   = getenv('KAFKA_TOPIC');

        if (! $this->version || ! $this->brokers || ! $this->topic) {
            self::markTestSkipped(
                'Environment variables "KAFKA_VERSION", "KAFKA_TOPIC", and "KAFKA_BROKERS" must be provided'
            );
        }

        $this->container = \Kafka\Producer::getContainer([
            \Kafka\Contracts\Config\Broker::class => function () {
                $config = new \Kafka\Config\Broker();
                $config->setMetadataBrokerList($this->brokers);
                $config->setVersion($this->version);
                return $config;
            },
            // configure sasl
            \Kafka\Contracts\Config\Sasl::class => function () {
                $config = new \Kafka\Config\Sasl();
                $config->setUsername('nmred');
                $config->setPassword('123456');
                return $config;
            },
            \Kafka\Contracts\Config\Consumer::class => function () {
                $config = new \Kafka\Config\Consumer();
                $config->setTopics([$this->topic]);
                $config->setGroupId('kafka-php-tests');
                $config->setOffsetReset('earliest');
                return $config;
            },
        ]);
    }

    /**
     * @test
     * @runInSeparateProcess
     */
    public function consumeProducedMessages(): void
    {
        $consumedMessages = 0;
        $executionEnd     = new \DateTimeImmutable('+1 minute');

        $stop = new Callback(
            function () use (&$consumedMessages, $executionEnd): bool {
                return $consumedMessages >= self::MESSAGES_TO_SEND || new \DateTimeImmutable() > $executionEnd;
            }
        );
        $consumer = $this->container->make(\Kafka\Consumer::class, ['stopStrategy' => $stop]);
        $consumer->start(
            function () use (&$consumedMessages) {
                ++$consumedMessages;
            }
        );

        $this->assertEquals(self::MESSAGES_TO_SEND, $consumedMessages);
    }

    public function createMessages(int $amount = self::MESSAGES_TO_SEND): array
    {
        $messages = [];

        for ($i = 0; $i < $amount; ++$i) {
            $messages[] = ['topic' => $this->topic, 'value' => 'msg-' . $i];
        }

        return $messages;
    }
}
