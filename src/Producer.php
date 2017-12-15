<?php
namespace Kafka;

use DI\FactoryInterface;
use Psr\Log\LoggerInterface;
use Kafka\Contracts\Producer\SyncInterface;
use Kafka\Contracts\AsynchronousProcess;
use Kafka\Contracts\StopStrategy;

class Producer extends Bootstrap implements AsynchronousProcess
{
    private $process = null;

    private $container = null;

    private $logger;

    private $stopStrategy = null;

    /**
     * __construct
     *
     * @access public
     */
    public function __construct(?callable $producer = null, FactoryInterface $container, LoggerInterface $logger, ?StopStrategy $stopStrategy = null)
    {
        $this->container = $container;
        $this->logger    = $logger;
        if (is_null($producer)) {
            $this->container->set(\Kafka\Contracts\SocketInterface::class, \DI\object(\Kafka\Socket\SocketBlocking::class));
            $this->process = $this->container->make(\Kafka\Producer\SyncProcess::class);
        } else {
            $this->container->set(\Kafka\Contracts\SocketInterface::class, \DI\object(\Kafka\Socket\SocketUnblocking::class));
            $this->process = $this->container->make(\Kafka\Producer\Process::class, ['producer' => $producer]);
        }
        $this->stopStrategy = $stopStrategy;
    }

    /**
     * start producer
     *
     * @access public
     * @data is data is boolean that is async process, thus it is sync process
     * @return void
     */
    public function send($data = true)
    {
        if (is_bool($data)) {
            $this->setupStopStrategy();
            $this->process->start();
            if ($data) {
                \Amp\Loop::run();
            }
        } else {
            return $this->process->send($data);
        }
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
        \Amp\Loop::stop();
    }

    /**
     * syncMeta producer
     *
     * @access public
     * @return void
     */
    public function syncMeta()
    {
        return $this->process->syncMeta();
    }

    /**
     * producer success
     *
     * @access public
     * @return void
     */
    public function success(callable $success = null)
    {
        $this->process->setSuccess($success);
    }
    /**
     * producer error
     *
     * @access public
     * @return void
     */
    public function error(callable $error = null)
    {
        $this->process->setError($error);
    }
}
