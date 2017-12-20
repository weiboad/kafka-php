<?php
declare(strict_types=1);

namespace KafkaTest\Base\StopStrategy;

use Kafka\Loop;
use Kafka\Consumer;
use Kafka\StopStrategy\Delay;
use PHPUnit\Framework\MockObject\MockObject;

final class DelayTest extends \PHPUnit\Framework\TestCase
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
    public function setupShouldStopTheConsumerAfterTheConfiguredDelay(): void
    {
        $this->consumer->expects($this->once())
                       ->method('stop');

        $strategy = new Delay(10, $this->loop);
        $strategy->setup($this->consumer);

        self::assertSame(1, $this->loop->getInfo()['delay']['enabled']);

        $this->loop->delay(20, [$this->loop, 'stop']);
        $this->loop->run();

        self::assertSame(0, $this->loop->getInfo()['delay']['enabled']);
    }
}
