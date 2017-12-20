<?php
namespace Kafka;

use Kafka\Loop;
use Psr\Log\LoggerInterface;
use DI\FactoryInterface;
use Kafka\Contracts\Consumer\Process;
use Kafka\Contracts\StopStrategy;
use Kafka\Contracts\AsynchronousProcess;

class Consumer extends Bootstrap implements AsynchronousProcess
{
    /**
     * @var StopStrategy|null
     */
    private $stopStrategy;

    /**
     * @var Process|null
     */
    private $process;

    private $logger;

    private $container;

    private $loop;

    public function __construct(FactoryInterface $container, ?StopStrategy $stopStrategy = null, LoggerInterface $logger, Loop $loop)
    {
        $this->stopStrategy = $stopStrategy;
        $this->logger       = $logger;
        $this->container    = $container;
        $this->loop         = $loop;
    }

    /**
     * start consumer
     *
     * @access public
     *
     * @param callable|null $consumer
     *
     * @return void
     */
    public function start(?callable $consumer = null): void
    {
        if ($this->process !== null) {
            $this->logger->error('Consumer is already being executed');
            return;
        }
        $this->setupStopStrategy();
        $this->process = $this->container->make(Consumer\Process::class, ['consumer' => $consumer]);
        $this->process->start();

        $this->loop->run();
    }

    private function setupStopStrategy(): void
    {
        if ($this->stopStrategy === null) {
            return;
        }

        $this->stopStrategy->setup($this);
    }

    public function stop(): void
    {
        if ($this->process === null) {
            $this->logger->error('Consumer is not running');
            return;
        }

        $this->process->stop();
        $this->process = null;

        $this->loop->stop();
    }
}
