<?php
namespace Kafka;

use \Kafka\Loop;

class Producer
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    private $process = null;

    private $loop = null;

    /**
     * __construct
     *
     * @access public
     * @param $hostList
     * @param null $timeout
     */
    public function __construct(callable $producer = null)
    {
        if (is_null($producer)) {
            $this->process = new \Kafka\Producer\SyncProcess();
        } else {
            $this->process = new \Kafka\Producer\Process($producer);
        }
        $this->loop = Loop::getInstance();
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
        if ($this->logger) {
            $this->process->setLogger($this->logger);
        }
        if (is_bool($data)) {
            $this->process->start();
            if ($data) {
                $this->loop->run();
            }
        } else {
            return $this->process->send($data);
        }
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
