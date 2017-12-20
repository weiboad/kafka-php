<?php
declare(strict_types=1);

namespace KafkaTest\Base\StopStrategy;

use Kafka\Loop;
use Kafka\Consumer;
use Kafka\StopStrategy\Callback;
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
    public function createConsumer(): void
    {
        $this->consumer = $this->createPartialMock(Consumer::class, ['stop']);
        $this->loop     = new Loop();
    }

    /**
     * @test
     */
    public function setupShouldStopTheConsumerOnceTheCallbackReturnsTrue(): void
    {
        $this->consumer->expects($this->once())
            ->method('stop')
            ->will($this->returnCallback(function () {
                $this->loop->stop();
            }));

        $executionCount = 0;

        $strategy = new Callback(
            function () use (&$executionCount): bool {
                return ++$executionCount === 5;
            },
            10,
            $this->loop
        );
        $strategy->setup($this->consumer);

        self::assertSame(1, $this->loop->getInfo()['repeat']['enabled']);

        $this->loop->run();

        self::assertSame(0, $this->loop->getInfo()['repeat']['enabled']);
    }
}
