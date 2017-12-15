<?php
namespace Kafka;

use Psr\Log\LoggerInterface;
use Kafka\Contracts\Producer\SyncInterface;

class Producer extends Bootstrap
{
    private $process = null;

    /**
     * __construct
     *
     * @access public
     * @param $hostList
     * @param null $timeout
     */
    public function __construct(SyncInterface $process)
    {
        $this->process = $process;
    }

    /**
     * start producer
     *
     * @access public
     * @data is data is boolean that is async process, thus it is sync process
     * @return void
     */
    public function send($data)
    {
        return $this->process->send($data);
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
}
