<?php

namespace KafkaTest\Base;

use Kafka\ConsumerConfig;
use PHPUnit\Framework\TestCase;

final class ConsumerConfigTest extends TestCase
{
    /**
     * @var ConsumerConfig
     */
    private $config;

    /**
     * @before
     */
    public function configureInstance(): void
    {
        $this->config = ConsumerConfig::getInstance();
    }

    /**
     * @after
     */
    public function cleanUpInstance(): void
    {
        ConsumerConfig::getInstance()->clear();
    }

    public function testDefaultConfig(): void
    {
        self::assertSame($this->config->getClientId(), 'kafka-php');
        self::assertSame($this->config->getSessionTimeout(), 30000);

        self::assertFalse($this->config->setValidKey('xxx', '222'));
        self::assertFalse($this->config->getValidKey());

        $this->config->setValidKey('222');
        self::assertSame($this->config->getValidKey(), '222');
    }

    public function testSetClientId(): void
    {
        $this->config->setClientId('kafka-php1');

        self::assertSame($this->config->getClientId(), 'kafka-php1');
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set clientId value is invalid, must is not empty string.
     */
    public function testSetClientIdEmpty(): void
    {
        $this->config->setClientId('');
    }

    public function testSetBrokerVersion(): void
    {
        $this->config->setBrokerVersion('0.9.0.1');

        self::assertSame($this->config->getBrokerVersion(), '0.9.0.1');
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set broker version value is invalid, must is not empty string and gt 0.8.0.
     */
    public function testSetBrokerVersionEmpty(): void
    {
        $this->config->setBrokerVersion('');
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set broker version value is invalid, must is not empty string and gt 0.8.0.
     */
    public function testSetBrokerVersionInvalid(): void
    {
        $this->config->setBrokerVersion('0.1');
    }

    public function testSetMetadataBrokerList(): void
    {
        $this->config->setMetadataBrokerList(' 127.0.0.1:9192,127.0.0.1:9292'); // with whitespace to ensure that the list is trimmed

        self::assertSame($this->config->getMetadataBrokerList(), '127.0.0.1:9192,127.0.0.1:9292');
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Broker list must be a comma-separated list of brokers (format: "host:port"), with at least one broker
     */
    public function testSetMetadataBrokerListEmpty(): void
    {
        $this->config->setMetadataBrokerList('');
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Broker list must be a comma-separated list of brokers (format: "host:port"), with at least one broker
     */
    public function testSetMetadataBrokerListEmpty1(): void
    {
        $this->config->setMetadataBrokerList(',');
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Broker list must be a comma-separated list of brokers (format: "host:port"), with at least one broker
     */
    public function testSetMetadataBrokerListEmpty2(): void
    {
        $this->config->setMetadataBrokerList('127.0.0.1: , : ');
    }

    public function testSetGroupId(): void
    {
        $this->config->setGroupId('test');

        self::assertSame($this->config->getGroupId(), 'test');
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set group id value is invalid, must set it not empty string
     */
    public function testSetGroupIdEmpty(): void
    {
        $this->config->setGroupId('');
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Get group id value is invalid, must set it not empty string
     */
    public function testGetGroupIdEmpty(): void
    {
        $this->config->getGroupId();
    }

    public function testSetSessionTimeout(): void
    {
        $this->config->setSessionTimeout(2000);

        self::assertSame($this->config->getSessionTimeout(), 2000);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set session timeout value is invalid, must set it 1 .. 3600000
     */
    public function testSetSessionTimeoutInvalid(): void
    {
        $this->config->setSessionTimeout('-1');
    }

    public function testSetRebalanceTimeout(): void
    {
        $this->config->setRebalanceTimeout(2000);

        self::assertSame($this->config->getRebalanceTimeout(), 2000);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set rebalance timeout value is invalid, must set it 1 .. 3600000
     */
    public function testSetRebalanceTimeoutInvalid(): void
    {
        $this->config->setRebalanceTimeout('-1');
    }

    public function testSetOffsetReset(): void
    {
        $this->config->setOffsetReset('earliest');

        self::assertSame($this->config->getOffsetReset(), 'earliest');
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set offset reset value is invalid, must set it `latest` or `earliest`
     */
    public function testSetOffsetResetInvalid(): void
    {
        $this->config->setOffsetReset('xxxx');
    }

    public function testSetTopics(): void
    {
        $this->config->setTopics(['test']);

        self::assertSame($this->config->getTopics(), ['test']);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set consumer topics value is invalid, must set it not empty array
     */
    public function testSetTopicsEmpty(): void
    {
        $this->config->setTopics([]);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Get consumer topics value is invalid, must set it not empty
     */
    public function testGetTopicsEmpty(): void
    {
        $this->config->getTopics();
    }

    public function testSetMessageMaxBytes(): void
    {
        $this->config->setMessageMaxBytes(1011);

        self::assertSame($this->config->getMessageMaxBytes(), 1011);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set message max bytes value is invalid, must set it 1000 .. 1000000000
     */
    public function testSetMessageMaxBytesInvalid(): void
    {
        $this->config->setMessageMaxBytes('999');
    }

    public function testSetMetadataRequestTimeoutMs(): void
    {
        $this->config->setMetadataRequestTimeoutMs(1011);

        self::assertSame($this->config->getMetadataRequestTimeoutMs(), 1011);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set metadata request timeout value is invalid, must set it 10 .. 900000
     */
    public function testSetMetadataRequestTimeoutMsInvalid(): void
    {
        $this->config->setMetadataRequestTimeoutMs('9');
    }

    public function testSetMetadataRefreshIntervalMs(): void
    {
        $this->config->setMetadataRefreshIntervalMs(1011);

        self::assertSame($this->config->getMetadataRefreshIntervalMs(), 1011);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set metadata refresh interval value is invalid, must set it 10 .. 3600000
     */
    public function testSetMetadataRefreshIntervalMsInvalid(): void
    {
        $this->config->setMetadataRefreshIntervalMs('9');
    }

    public function testSetMetadataMaxAgeMs(): void
    {
        $this->config->setMetadataMaxAgeMs(1011);

        self::assertSame($this->config->getMetadataMaxAgeMs(), 1011);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set metadata max age value is invalid, must set it 1 .. 86400000
     */
    public function testSetMetadataMaxAgeMsInvalid(): void
    {
        $this->config->setMetadataMaxAgeMs('86400001');
    }
}
