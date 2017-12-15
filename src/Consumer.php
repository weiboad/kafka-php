<?php
namespace Kafka;

use Amp\Loop;
use Psr\Log\LoggerInterface;
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

    public function __construct(Process $process, ?StopStrategy $stopStrategy = null, LoggerInterface $logger)
    {
        $this->process = $process;
        $this->stopStrategy = $stopStrategy;
		$this->logger = $logger;
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
        $this->setupStopStrategy();

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
            $this->error('Consumer is not running');
            return;
        }

        $this->process->stop();
        $this->process = null;

        Loop::stop();
    }
}
