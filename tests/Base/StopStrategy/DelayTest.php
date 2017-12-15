<?php
declare(strict_types=1);

namespace KafkaTest\Base\StopStrategy;

use Amp\Loop;
use Kafka\Consumer;
use Kafka\StopStrategy\Delay;
use PHPUnit\Framework\MockObject\MockObject;

final class DelayTest extends \PHPUnit\Framework\TestCase
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
    public function setupShouldStopTheConsumerAfterTheConfiguredDelay(): void
    {
        $this->consumer->expects($this->once())
                       ->method('stop');

        $strategy = new Delay(10);
        $strategy->setup($this->consumer);

        self::assertSame(1, Loop::getInfo()['delay']['enabled']);

        Loop::delay(20, [Loop::class, 'stop']);
        Loop::run();

        self::assertSame(0, Loop::getInfo()['delay']['enabled']);
    }
}
