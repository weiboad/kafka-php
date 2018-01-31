<?php
namespace Kafka;

use Kafka\Loop;
use Kafka\Consumer\Process;
use Kafka\Consumer\StopStrategy;

class Consumer
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    /**
     * @var StopStrategy|null
     */
    private $stopStrategy;

    private $loop = null;

    /**
     * @var Process|null
     */
    private $process;

    public function __construct($stopStrategy = null)
    {
        $this->stopStrategy = $stopStrategy;
        $this->loop         = Loop::getInstance();
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
    public function start($consumer = null)
    {
        if ($this->process !== null) {
            $this->error('Consumer is already being executed');
            return;
        }

        $this->setupStopStrategy();

        $this->process = $this->createProcess($consumer);
        $this->process->start();

        $this->loop->run();
    }

    /**
     * FIXME: remove it when we implement dependency injection
     *
     * This is a very bad practice, but if we don't create this method
     * this class will never be testable...
     *
     * @codeCoverageIgnore
     */
    protected function createProcess($consumer)
    {
        $process = new Process($consumer);

        if ($this->logger) {
            $process->setLogger($this->logger);
        }

        return $process;
    }

    private function setupStopStrategy()
    {
        if ($this->stopStrategy === null) {
            return;
        }

        $this->stopStrategy->setup($this);
    }

    public function stop()
    {
        if ($this->process === null) {
            $this->error('Consumer is not running');
            return;
        }

        $this->process->stop();
        $this->process = null;

        $this->loop->stop();
    }
}
