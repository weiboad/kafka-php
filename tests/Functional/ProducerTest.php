<?php
declare(strict_types=1);

namespace KafkaTest\Functional;

use Kafka\Consumer\StopStrategy\Callback;

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
    }

    protected function configureProducer(): void
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setMetadataBrokerList($this->brokers);
        $config->setBrokerVersion($this->version);
    }

    /**
     * @test
     *
     * @runInSeparateProcess
     */
    public function consumeProducedMessages(): void
    {
        $this->configureConsumer();

        $consumedMessages = 0;
        $executionEnd     = new \DateTimeImmutable('+1 minute');

        $consumer = new \Kafka\Consumer(
            new Callback(
                function () use (&$consumedMessages, $executionEnd): bool {
                    return $consumedMessages >= self::MESSAGES_TO_SEND || new \DateTimeImmutable() > $executionEnd;
                }
            )
        );

        $consumer->start(
            function () use (&$consumedMessages) {
                ++$consumedMessages;
            }
        );

        self::assertSame(self::MESSAGES_TO_SEND, $consumedMessages);
    }

    private function configureConsumer(): void
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataBrokerList($this->brokers);
        $config->setBrokerVersion($this->version);
        $config->setGroupId('kafka-php-tests');
        $config->setOffsetReset('earliest');
        $config->setTopics([$this->topic]);
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
