<?php
namespace Kafka\Config;

use Kafka\Exception;
use Kafka\Contracts\Config\Socket as SocketInterface;

class Socket implements SocketInterface
{
    /**
     * Send timeout in seconds.
     *
     * @var float
     * @access protected
     */
    protected $sendTimeoutSec = 0;

    /**
     * Send timeout in microseconds.
     *
     * @var float
     * @access protected
     */
    protected $sendTimeoutUsec = 100000;

    /**
     * Recv timeout in seconds
     *
     * @var float
     * @access protected
     */
    protected $recvTimeoutSec = 0;

    /**
     * Recv timeout in microseconds
     *
     * @var float
     * @access protected
     */
    protected $recvTimeoutUsec = 750000;

    /**
     * Max Write Attempts
     * @var int
     * @access protected
     */
    protected $maxWriteAttempts = 3;

    public function getSendTimeoutSec() : int
    {
        return $this->sendTimeoutSec;
    }

    public function getSendTimeoutUsec() : int
    {
        return $this->sendTimeoutUsec;
    }

    public function getRecvTimeoutSec() : int
    {
        return $this->recvTimeoutSec;
    }

    public function getRecvTimeoutUsec() : int
    {
        return $this->recvTimeoutUsec;
    }

    public function getMaxWriteAttempts() : int
    {
        return $this->maxWriteAttempts;
    }

    /**
     * @param float $sendTimeoutSec
     */
    public function setSendTimeoutSec(int $sendTimeoutSec) : void
    {
        $this->sendTimeoutSec = $sendTimeoutSec;
    }

    /**
     * @param float $sendTimeoutUsec
     */
    public function setSendTimeoutUsec(int $sendTimeoutUsec) : void
    {
        $this->sendTimeoutUsec = $sendTimeoutUsec;
    }

    /**
     * @param float $recvTimeoutSec
     */
    public function setRecvTimeoutSec(int $recvTimeoutSec) : void
    {
        $this->recvTimeoutSec = $recvTimeoutSec;
    }
    
    /**
     * @param float $recvTimeoutUsec
     */
    public function setRecvTimeoutUsec(int $recvTimeoutUsec) : void
    {
        $this->recvTimeoutUsec = $recvTimeoutUsec;
    }

    /**
     * @param int $number
     */
    public function setMaxWriteAttempts(int $number) : void
    {
        $this->maxWriteAttempts = $number;
    }
}
