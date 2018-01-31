<?php
namespace KafkaTest\Base\Consumer\StopStrategy;

use Kafka\Loop;
use Kafka\Consumer;
use Kafka\Consumer\StopStrategy\Callback;
use PHPUnit\Framework\MockObject\MockObject;

final class CallbackTest extends \PHPUnit\Framework\TestCase
{
    /**
     * @var Consumer|MockObject
     */
    private $consumer;

    private $loop;

    /**
     * @before
     */
    public function createConsumer()
    {
        $this->consumer = $this->createMock(Consumer::class, ['stop']);
        $this->loop     = \Kafka\Loop::getInstance();
    }

    /**
     * @test
     */
    public function setupShouldStopTheConsumerOnceTheCallbackReturnsTrue()
    {
        $this->consumer->expects($this->once())
                       ->method('stop');

        $executionCount = 0;

        $strategy = new Callback(
            function () use (&$executionCount) {
                return ++$executionCount === 5;
            },
            10
        );
        $strategy->setup($this->consumer);

        self::assertSame(1, $this->loop->getInfo()['repeat']['enabled']);

        $this->loop->delay(60, [$this->loop, 'stop']);
        $this->loop->run();

        self::assertSame(0, $this->loop->getInfo()['repeat']['enabled']);
    }
}
