<?php
declare(strict_types=1);

namespace KafkaTest\Base;

use Kafka\ConsumeMode;
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
        $this->config->setSessionTimeout(-1);
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
        $this->config->setRebalanceTimeout(-1);
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

    /**
     * @test
     */
    public function setConsumeModeShouldConfigureTheAttributeProperly(): void
    {
        self::assertSame(ConsumeMode::consumeAfterCommitOffset(), $this->config->getConsumeMode());

        $this->config->setConsumeMode(ConsumeMode::consumeBeforeCommitOffset());

        self::assertSame(ConsumeMode::consumeBeforeCommitOffset(), $this->config->getConsumeMode());
    }
}
