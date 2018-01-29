<?php
declare(strict_types=1);

namespace KafkaTest\Functional;

use Kafka\Consumer;
use Kafka\Consumer\StopStrategy\Callback;
use Kafka\ConsumerConfig;
use Kafka\ProducerConfig;
use Kafka\Protocol\Protocol;
use PHPUnit\Framework\TestCase;
use const STR_PAD_LEFT;
use function getenv;
use function str_pad;
use function version_compare;

abstract class ProducerTest extends TestCase
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
     * @var bool
     */
    protected $compress;

    /**
     * @before
     */
    public function prepareEnvironment(): void
    {
        $this->version  = getenv('KAFKA_VERSION');
        $this->brokers  = getenv('KAFKA_BROKERS');
        $this->topic    = getenv('KAFKA_TOPIC');
        $this->compress = getenv('KAFKA_COMPRESS') === '1';

        if (! $this->version || ! $this->brokers || ! $this->topic) {
            self::markTestSkipped(
                'Environment variables "KAFKA_VERSION", "KAFKA_TOPIC", and "KAFKA_BROKERS" must be provided'
            );
        }
    }

    protected function configureProducer(): void
    {
        /** @var ProducerConfig $config */
        $config = ProducerConfig::getInstance();
        $config->setMetadataBrokerList($this->brokers);
        $config->setBrokerVersion($this->version);

        if ($this->compress) {
            $config->setCompression(Protocol::COMPRESSION_GZIP);
        }
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

        $consumer = new Consumer(
            new Callback(
                function () use (&$consumedMessages, $executionEnd): bool {
                    return $consumedMessages >= self::MESSAGES_TO_SEND || new \DateTimeImmutable() > $executionEnd;
                }
            )
        );

        $consumer->start(
            function (string $topic, int $partition, array $message) use (&$consumedMessages): void {
                self::assertSame($this->topic, $topic);
                self::assertLessThan(3, $partition);
                self::assertArrayHasKey('offset', $message);
                self::assertArrayHasKey('size', $message);
                self::assertArrayHasKey('message', $message);
                self::assertArrayHasKey('crc', $message['message']);
                self::assertArrayHasKey('magic', $message['message']);
                self::assertArrayHasKey('attr', $message['message']);
                self::assertArrayHasKey('key', $message['message']);
                self::assertArrayHasKey('value', $message['message']);
                self::assertContains('msg-', $message['message']['value']);

                if (version_compare($this->version, '0.10.0', '>=')) {
                    self::assertArrayHasKey('timestamp', $message['message']);
                    self::assertNotEquals(-1, $message['message']['timestamp']);
                }

                ++$consumedMessages;
            }
        );

        self::assertSame(self::MESSAGES_TO_SEND, $consumedMessages);
    }

    private function configureConsumer(): void
    {
        /** @var ConsumerConfig $config */
        $config = ConsumerConfig::getInstance();
        $config->setMetadataBrokerList($this->brokers);
        $config->setBrokerVersion($this->version);
        $config->setGroupId('kafka-php-tests');
        $config->setOffsetReset('earliest');
        $config->setTopics([$this->topic]);
    }

    /**
     * @return string[][]
     */
    public function createMessages(int $amount = self::MESSAGES_TO_SEND): array
    {
        $messages = [];

        for ($i = 0; $i < $amount; ++$i) {
            $messages[] = [
                'topic' => $this->topic,
                'value' => 'msg-' . str_pad((string) ($i + 1), 2, '0', STR_PAD_LEFT),
            ];
        }

        return $messages;
    }
}
