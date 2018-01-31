<?php
namespace KafkaTest\Base;

use Kafka\Loop;
use Kafka\Consumer;
use PHPUnit\Framework\MockObject\MockObject;

final class ConsumerTest extends \PHPUnit\Framework\TestCase
{
    /**
     * @var Consumer|MockObject
     */
    private $consumer;

    /**
     * @before
     */
    public function createConsumer($stopStrategy = null)
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
    public function startShouldRunTheProcessForeverWhenNoStopStrategyWasConfigured()
    {
        $executed = false;

        $callback = function () {
        };

        $startCallback = function () use (&$executed) {
            $executed = true;
            Loop::getInstance()->stop();
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
    public function startShouldLogErrorWhenSomeoneTriesToDoItTwice()
    {
        $callback = function () {
        };

        $startCallback = function () {
            $this->consumer->start();
            Loop::getInstance()->stop();
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
    public function startShouldRunTheProcessUntilTheStopVerifierSaysSo()
    {
        $executed = false;

        $callback = function () {
        };

        $startCallback = function () use (&$executed) {
            $executed = true;
        };

        $this->createConsumer(
            new Consumer\StopStrategy\Callback(
                function () use (&$executed) {
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
    public function stopShouldCleanThingsAndStopTheLoop()
    {
        $callback = function () {
        };

        $startCallback = function () {
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
    public function stopShouldLogErrorWhenConsumerIsNotRunning()
    {
        $this->consumer->expects($this->once())
                       ->method('error')
                       ->with('Consumer is not running');

        $this->consumer->stop();
    }

    private function createProcess($startCallback, $expectsStop = false)
    {
        $process = $this->createMock(Consumer\Process::class);

        $process->method('start')->willReturnCallback(
            function () use ($startCallback) {
				Loop::getInstance()->defer($startCallback);
            }
        );

        $process->expects($expectsStop ? $this->once() : $this->never())
                ->method('stop');

        return $process;
    }
}
