<?php
declare(strict_types=1);

namespace KafkaTest\Base;

use Kafka\ConsumerConfig;
use Kafka\Exception\Config;
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

    public function testSetGroupIdEmpty(): void
    {
        $this->expectExceptionMessage("Set group id value is invalid, must set it not empty string");
        $this->expectException(Config::class);
        $this->config->setGroupId('');
    }

    public function testGetGroupIdEmpty(): void
    {
        $this->expectExceptionMessage("Get group id value is invalid, must set it not empty string");
        $this->expectException(Config::class);
        $this->config->getGroupId();
    }

    public function testSetSessionTimeout(): void
    {
        $this->config->setSessionTimeout(2000);

        self::assertSame($this->config->getSessionTimeout(), 2000);
    }

    public function testSetSessionTimeoutInvalid(): void
    {
        $this->expectExceptionMessage("Set session timeout value is invalid, must set it 1 .. 3600000");
        $this->expectException(Config::class);
        $this->config->setSessionTimeout(-1);
    }

    public function testSetRebalanceTimeout(): void
    {
        $this->config->setRebalanceTimeout(2000);

        self::assertSame($this->config->getRebalanceTimeout(), 2000);
    }

    public function testSetRebalanceTimeoutInvalid(): void
    {
        $this->expectExceptionMessage("Set rebalance timeout value is invalid, must set it 1 .. 3600000");
        $this->expectException(Config::class);
        $this->config->setRebalanceTimeout(-1);
    }

    public function testSetOffsetReset(): void
    {
        $this->config->setOffsetReset('earliest');

        self::assertSame($this->config->getOffsetReset(), 'earliest');
    }

    public function testSetOffsetResetInvalid(): void
    {
        $this->expectExceptionMessage("Set offset reset value is invalid, must set it `latest` or `earliest`");
        $this->expectException(Config::class);
        $this->config->setOffsetReset('xxxx');
    }

    public function testSetTopics(): void
    {
        $this->config->setTopics(['test']);

        self::assertSame($this->config->getTopics(), ['test']);
    }

    public function testSetTopicsEmpty(): void
    {
        $this->expectExceptionMessage("Set consumer topics value is invalid, must set it not empty array");
        $this->expectException(Config::class);
        $this->config->setTopics([]);
    }

    public function testGetTopicsEmpty(): void
    {
        $this->expectExceptionMessage("Get consumer topics value is invalid, must set it not empty");
        $this->expectException(Config::class);
        $this->config->getTopics();
    }

    /**
     * @test
     */
    public function setConsumeModeShouldConfigureTheAttributeProperly(): void
    {
        self::assertSame(ConsumerConfig::CONSUME_AFTER_COMMIT_OFFSET, $this->config->getConsumeMode());

        $this->config->setConsumeMode(ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET);

        self::assertSame(ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET, $this->config->getConsumeMode());
    }

    /**
     * @test
     */
    public function setShouldThrowExceptionWhenInvalidDataIsGiven(): void
    {
        $this->expectException(Config::class);

        $this->config->setConsumeMode(100);
    }
}
