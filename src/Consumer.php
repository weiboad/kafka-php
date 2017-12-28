<?php
namespace Kafka;

use Amp\Loop;
use Kafka\Consumer\Process;
use Kafka\Consumer\StopStrategy;
use Psr\Log\LoggerAwareTrait;

class Consumer
{
    use LoggerAwareTrait;
    use LoggerTrait;

    /**
     * @var StopStrategy|null
     */
    private $stopStrategy;

    /**
     * @var Process|null
     */
    private $process;

    public function __construct(?StopStrategy $stopStrategy = null)
    {
        $this->stopStrategy = $stopStrategy;
    }

    /**
     * start consumer
     *
     * @access public
     *
     *
     */
    public function start(?callable $consumer = null): void
    {
        if ($this->process !== null) {
            $this->error('Consumer is already being executed');
            return;
        }

        $this->setupStopStrategy();

        $this->process = $this->createProcess($consumer);
        $this->process->start();

        Loop::run();
    }

    /**
     * FIXME: remove it when we implement dependency injection
     *
     * This is a very bad practice, but if we don't create this method
     * this class will never be testable...
     *
     * @codeCoverageIgnore
     */
    protected function createProcess(?callable $consumer): Process
    {
        $process = new Process($consumer);

        if ($this->logger) {
            $process->setLogger($this->logger);
        }

        return $process;
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
