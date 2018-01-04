<?php
declare(strict_types=1);

namespace KafkaTest\Base;

use Amp\Loop;
use Kafka\Consumer;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

final class ConsumerTest extends TestCase
{
    /**
     * @var Consumer|MockObject
     */
    private $consumer;

    /**
     * @before
     */
    public function createConsumer(?Consumer\StopStrategy $stopStrategy = null): void
    {
        $this->consumer = $this->getMockBuilder(Consumer::class)
                               ->disableOriginalClone()
                               ->disableArgumentCloning()
                               ->disallowMockingUnknownTypes()
                               ->setConstructorArgs([$stopStrategy])
                               ->setMethods(['createProcess', 'error'])
                               ->getMock();
    }

    /**
     * @test
     */
    public function startShouldRunTheProcessForeverWhenNoStopStrategyWasConfigured(): void
    {
        $executed = false;

        $callback = function (): void {
        };

        $startCallback = function () use (&$executed): void {
            $executed = true;
            Loop::stop();
        };

        $this->consumer->expects($this->once())
                       ->method('createProcess')
                       ->with($callback)
                       ->willReturn($this->createProcess($startCallback));

        $this->consumer->start($callback);

        self::assertTrue($executed);
    }

    /**
     * @test
     */
    public function startShouldLogErrorWhenSomeoneTriesToDoItTwice(): void
    {
        $callback = function (): void {
        };

        $startCallback = function (): void {
            $this->consumer->start();
            Loop::stop();
        };

        $this->consumer->expects($this->once())
                       ->method('error')
                       ->with('Consumer is already being executed');

        $this->consumer->expects($this->once())
                       ->method('createProcess')
                       ->with($callback)
                       ->willReturn($this->createProcess($startCallback));

        $this->consumer->start($callback);
    }

    /**
     * @test
     */
    public function startShouldRunTheProcessUntilTheStopVerifierSaysSo(): void
    {
        $executed = false;

        $callback = function (): void {
        };

        $startCallback = function () use (&$executed): void {
            $executed = true;
        };

        $this->createConsumer(
            new Consumer\StopStrategy\Callback(
                function () use (&$executed): bool {
                    return $executed;
                }
            )
        );

        $this->consumer->expects($this->once())
                       ->method('createProcess')
                       ->with($callback)
                       ->willReturn($this->createProcess($startCallback, true));

        $this->consumer->start($callback);

        self::assertTrue($executed);
    }

    /**
     * @test
     */
    public function stopShouldCleanThingsAndStopTheLoop(): void
    {
        $callback = function (): void {
        };

        $startCallback = function (): void {
            $this->consumer->stop();
        };

        $this->consumer->expects($this->once())
                       ->method('createProcess')
                       ->with($callback)
                       ->willReturn($this->createProcess($startCallback, true));

        $this->consumer->start($callback);
    }

    /**
     * @test
     */
    public function stopShouldLogErrorWhenConsumerIsNotRunning(): void
    {
        $this->consumer->expects($this->once())
                       ->method('error')
                       ->with('Consumer is not running');

        $this->consumer->stop();
    }

    private function createProcess(callable $startCallback, bool $expectsStop = false): Consumer\Process
    {
        $process = $this->createMock(Consumer\Process::class);

        $process->method('start')->willReturnCallback(
            function () use ($startCallback): void {
                Loop::defer($startCallback);
            }
        );

        $process->expects($expectsStop ? $this->once() : $this->never())
                ->method('stop');

        return $process;
    }
}
