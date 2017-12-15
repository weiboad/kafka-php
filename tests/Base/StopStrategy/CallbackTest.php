<?php
declare(strict_types=1);

namespace KafkaTest\Base\StopStrategy;

use Amp\Loop;
use Kafka\Consumer;
use Kafka\StopStrategy\Callback;
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
    public function createConsumer(): void
    {
        $this->consumer = $this->createPartialMock(Consumer::class, ['stop']);
    }

    /**
     * @test
     */
    public function setupShouldStopTheConsumerOnceTheCallbackReturnsTrue(): void
    {
        $this->consumer->expects($this->once())
			->method('stop')
			->will($this->returnCallback(function() {
				Loop::stop();	
			}));

        $executionCount = 0;

        $strategy = new Callback(
            function () use (&$executionCount): bool {
                return ++$executionCount === 5;
            },
            10
        );
        $strategy->setup($this->consumer);

        self::assertSame(1, Loop::getInfo()['repeat']['enabled']);

        Loop::run();

        self::assertSame(0, Loop::getInfo()['repeat']['enabled']);
    }
}
