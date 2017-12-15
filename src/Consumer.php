<?php
namespace Kafka;

use Amp\Loop;
use Psr\Log\LoggerInterface;
use DI\FactoryInterface;
use Kafka\Contracts\Consumer\Process;
use Kafka\Contracts\Consumer\StopStrategy;

class Consumer extends Bootstrap
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

    public function __construct(FactoryInterface $container, ?StopStrategy $stopStrategy = null, LoggerInterface $logger)
    {
        $this->stopStrategy = $stopStrategy;
        $this->logger       = $logger;
        $this->container    = $container;
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

        Loop::run();
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

        Loop::stop();
    }
}
