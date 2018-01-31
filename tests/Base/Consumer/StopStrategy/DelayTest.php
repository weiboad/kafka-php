<?php
namespace KafkaTest\Base\Consumer\StopStrategy;

use Kafka\Loop;
use Kafka\Consumer;
use Kafka\Consumer\StopStrategy\Delay;
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
    public function createConsumer()
    {
        $this->consumer = $this->createPartialMock(Consumer::class, ['stop']);
        $this->loop     = Loop::getInstance();
    }

    /**
     * @test
     */
    public function setupShouldStopTheConsumerAfterTheConfiguredDelay()
    {
        $this->consumer->expects($this->once())
                       ->method('stop');

        $strategy = new Delay(10);
        $strategy->setup($this->consumer);

        self::assertSame(1, $this->loop->getInfo()['once']['enabled']);

        $this->loop->delay(20, [$this->loop, 'stop']);
        $this->loop->run();

        self::assertSame(0, $this->loop->getInfo()['once']['enabled']);
    }
}
