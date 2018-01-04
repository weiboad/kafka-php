<?php
declare(strict_types=1);

namespace KafkaTest\Base;

use Kafka\Exception\Config;
use Kafka\ProducerConfig;
use Kafka\Protocol\Produce;
use PHPUnit\Framework\TestCase;

final class ProducerConfigTest extends TestCase
{
    /**
     * @var ProducerConfig
     */
    private $config;

    /**
     * @before
     */
    public function configureInstance(): void
    {
        $this->config = ProducerConfig::getInstance();
    }

    /**
     * @after
     */
    public function cleanUpInstance(): void
    {
        ProducerConfig::getInstance()->clear();
    }

    public function testSetRequestTimeout(): void
    {
        $this->config->setRequestTimeout(1011);

        self::assertSame($this->config->getRequestTimeout(), 1011);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set request timeout value is invalid, must set it 1 .. 900000
     */
    public function testSetRequestTimeoutValid(): void
    {
        $this->config->setRequestTimeout(-1);
    }

    public function testSetProduceInterval(): void
    {
        $this->config->setProduceInterval(1011);

        self::assertSame($this->config->getProduceInterval(), 1011);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set produce interval timeout value is invalid, must set it 1 .. 900000
     */
    public function testSetProduceIntervalValid(): void
    {
        $this->config->setProduceInterval(-1);
    }

    public function testSetTimeout(): void
    {
        $this->config->setTimeout(1011);

        self::assertSame($this->config->getTimeout(), 1011);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set timeout value is invalid, must set it 1 .. 900000
     */
    public function testSetTimeoutValid(): void
    {
        $this->config->setTimeout(-1);
    }

    public function testSetRequiredAck(): void
    {
        $this->config->setRequiredAck(1);
        self::assertSame($this->config->getRequiredAck(), 1);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set required ack value is invalid, must set it -1 .. 1000
     */
    public function testSetRequiredAckValid(): void
    {
        $this->config->setRequiredAck(-2);
    }

    public function testSetIsAsyn(): void
    {
        $this->config->setIsAsyn(true);

        $this->assertTrue($this->config->getIsAsyn());
    }

    /**
     * @test
     */
    public function defaultValueForCompressionIsNull(): void
    {
        self::assertSame(Produce::COMPRESSION_NONE, $this->config->getCompression());
    }

    /**
     * @test
     */
    public function compressionCanBeConfigured(): void
    {
        $this->config->setCompression(Produce::COMPRESSION_GZIP);

        self::assertSame(Produce::COMPRESSION_GZIP, $this->config->getCompression());
    }

    /**
     * @test
     */
    public function compressionCanOnlyBeConfiguredUsingAValidOption(): void
    {
        $this->config->setCompression(Produce::COMPRESSION_GZIP);

        self::assertSame(Produce::COMPRESSION_GZIP, $this->config->getCompression());
    }

    /**
     * @test
     */
    public function setCompressionShouldRaiseExceptionWhenInvalidDataIsGiven(): void
    {
        $this->expectException(Config::class);

        $this->config->setCompression(123);
    }
}
