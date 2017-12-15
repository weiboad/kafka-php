<?php
declare(strict_types=1);

namespace KafkaTest\Base;

use Amp\Loop;
use Kafka\Consumer;
use Kafka\Contracts\Consumer\StopStrategy;
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
    public function createConsumer($definions = []): void
    {
        $builder = new \DI\ContainerBuilder();
        $builder->useAnnotations(false);
        $definions = array_merge([
            \Kafka\Contracts\Consumer\StopStrategy::class => null,
            \Psr\Log\LoggerInterface::class => \DI\object(\Psr\Log\NullLogger::class)
        ], $definions);
        $builder->addDefinitions($definions);
        $container      = $builder->build();
        $this->consumer = $container->get(\Kafka\Consumer::class);
    }

    /**
     * @test
     */
    public function startShouldRunTheProcessForeverWhenNoStopStrategyWasConfigured(): void
    {
        $executed = false;

        $callback = function () {
        };

        $startCallback = function () use (&$executed) {
            $executed = true;
            Loop::stop();
        };
        $this->createConsumer([
            \Kafka\Consumer\Process::class => function () use ($startCallback) {
                return $this->createProcess($startCallback);
            },
        ]);

        $this->consumer->start($callback);

        self::assertTrue($executed);
    }

    /**
     * @test
     */
    public function startShouldLogErrorWhenSomeoneTriesToDoItTwice(): void
    {
        $logger = $this->createMock(\Psr\Log\LoggerInterface::class);
        $logger->expects($this->once())
               ->method('error')
               ->with('Consumer is already being executed');

        $startCallback = function () {
            $this->consumer->start();
            Loop::stop();
        };

        $this->createConsumer([
            \Kafka\Consumer\Process::class => function () use ($startCallback) {
                return $this->createProcess($startCallback);
            },
            \Psr\Log\LoggerInterface::class => function () use ($logger) {
                return $logger;
            },
        ]);

        $this->consumer->start(function () {
        });
    }

    /**
     * @test
     */
    public function startShouldRunTheProcessUntilTheStopVerifierSaysSo(): void
    {
        $startCallback = function () {
            $this->consumer->stop();
        };


        $this->createConsumer([
            \Kafka\Consumer\Process::class => function () use ($startCallback) {
                return $this->createProcess($startCallback, true);
            },
        ]);

        $this->consumer->start(function () {
        });
    }

    /**
     * @test
     */
    public function stopShouldCleanThingsAndStopTheLoop(): void
    {
        $callback = function () {
        };

        $startCallback = function () {
            $this->consumer->stop();
        };

        $this->createConsumer([
            \Kafka\Consumer\Process::class => function () use ($startCallback) {
                return $this->createProcess($startCallback, true);
            },
        ]);

        $this->consumer->start($callback);
    }

    /**
     * @test
     */
    public function stopShouldLogErrorWhenConsumerIsNotRunning(): void
    {
        $logger = $this->createMock(\Psr\Log\LoggerInterface::class);
        $logger->expects($this->once())
               ->method('error')
               ->with('Consumer is not running');
        $this->createConsumer([
            \Psr\Log\LoggerInterface::class => function () use ($logger) {
                return $logger;
            },
        ]);
        $this->consumer->stop();
    }

    private function createProcess(callable $startCallback, bool $expectsStop = false): Consumer\Process
    {
        $process = $this->createMock(Consumer\Process::class);

        $process->method('start')->willReturnCallback(
            function () use ($startCallback) {
                Loop::defer($startCallback);
            }
        );

        $process->expects($expectsStop ? $this->once() : $this->never())
                ->method('stop');

        return $process;
    }
}
