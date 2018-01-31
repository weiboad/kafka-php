<?php
namespace KafkaTest\Base\Consumer\StopStrategy;

use Amp\Loop;
use Kafka\Consumer;
use Kafka\Consumer\StopStrategy\Callback;
use PHPUnit\Framework\MockObject\MockObject;

final class CallbackTest extends \PHPUnit\Framework\TestCase
{
    /**
     * @var Consumer|MockObject
     */
    private $consumer;

    /**
     * @before
     */
    public function createConsumer()
    {
        $this->consumer = $this->createMock(Consumer::class, ['stop']);
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

        self::assertSame(1, Loop::getInfo()['repeat']['enabled']);

        Loop::delay(60, [Loop::class, 'stop']);
        Loop::run();

        self::assertSame(0, Loop::getInfo()['repeat']['enabled']);
    }
}
