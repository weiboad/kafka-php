<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace Kafka\Broker;

/**
+------------------------------------------------------------------------------
* Kafka broker async connect manager
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Broker
{
    // {{{ consts
    // }}}
    // {{{ members

    /**
     * broker list
     *
     * @var mixed
     * @access private
     */
    private $brokerList = null;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    /**
     * __construct
     *
     * @access public
     * @param $brokerList
     */
    public function __construct($brokerList)
    {
        $this->brokerList = $brokerList;
    }

    /**
     * @param float $sendTimeoutSec
     */
    public function setSendTimeoutSec($sendTimeoutSec)
    {
        $this->sendTimeoutSec = $sendTimeoutSec;
    }

    /**
     * @param float $sendTimeoutUsec
     */
    public function setSendTimeoutUsec($sendTimeoutUsec)
    {
        $this->sendTimeoutUsec = $sendTimeoutUsec;
    }

    /**
     * @param float $recvTimeoutSec
     */
    public function setRecvTimeoutSec($recvTimeoutSec)
    {
        $this->recvTimeoutSec = $recvTimeoutSec;
    }

    /**
     * @param float $recvTimeoutUsec
     */
    public function setRecvTimeoutUsec($recvTimeoutUsec)
    {
        $this->recvTimeoutUsec = $recvTimeoutUsec;
    }

    /**
     * @param int $number
     */
    public function setMaxWriteAttempts($number)
    {
        $this->maxWriteAttempts = $number;
    }

    // }}}
    // {{{ public function init()

    /**
     * 
     * start broker
     *
     * @access public
     * @return void
     */
    public function init()
    {
        $brokerList = explode(',', $this->brokerList);
        $brokerHosts = array();
        foreach($brokerList as $brokerHost) {
            $brokerHosts[] = explode(':', trim($brokerHost)); 
        }
        shuffle($brokerHosts);
        foreach($brokerHosts as $brokerHost) {
            $socket = new \Kafka\Socket($brokerHost[0], $brokerHost[1]); 
            $socket->write();
        }
    }

    // }}}
    // {{{ public function start()

    /**
     * 
     * start broker
     *
     * @access public
     * @return void
     */
    public function start()
    {
        $this->init();
    }

    // }}}
    // {{{ public function reconnect()

    /**
     * reconnect the socket
     *
     * @access public
     * @return void
     */
    public function reconnect()
    {
        $this->close();
        $this->connect();
    }

    // }}}
    // {{{ public function SetOnReadable()

    /**
     * set on readable callback function
     *
     * @access public
     * @return void
     */
    public function SetOnReadable(\Closure $read)
    {
        $this->onReadable = $read;
    }

    // }}}
    // {{{ public function close()

    /**
     * close the socket
     *
     * @access public
     * @return void
     */
    public function close()
    {
        \Amp\cancel($this->readWatcher);
        \Amp\cancel($this->writeWatcher);
        if (is_resource($this->stream)) {
            fclose($this->stream);
        }
        $this->readBuffer = '';
        $this->writeBuffer = '';
        $this->readNeedLength = 0;
    }

    /**
     * checks if the socket is a valid resource
     *
     * @access public
     * @return boolean
     */
    public function isResource()
    {
        return is_resource($this->stream);
    }

    // }}}
    // {{{ public function read()

    /**
     * Read from the socket at most $len bytes.
     *
     * This method will not wait for all the requested data, it will return as
     * soon as any data is received.
     *
     * @param integer $len               Maximum number of bytes to read.
     * @param boolean $verifyExactLength Throw an exception if the number of read bytes is less than $len
     *
     * @return string Binary data
     * @throws \Kafka\Exception\SocketEOF
     */
    public function read($data)
    {
        $this->readBuffer .= $data;
        if ($this->readNeedLength == 0) { // response start
            if (strlen($this->readBuffer) < 4) {
                return;
            }
            $dataLen = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($this->readBuffer, 0, 4));
            $this->readNeedLength = $dataLen;
            $this->readBuffer = substr($this->readBuffer, 4);
        }

        if (strlen($this->readBuffer) < $this->readNeedLength) {
            return; 
        }
        $data = $this->readBuffer;
        $this->readNeedLength = 0;
        $this->readBuffer = '';
        call_user_func($this->onReadable, $data);
    }

    // }}}
    // {{{ public function write()

    /**
     * Write to the socket.
     *
     * @param string $buf The data to write
     *
     * @return integer
     * @throws \Kafka\Exception\SocketEOF
     */
    public function write($data = null)
    {
        if ($data != null) {
            $this->writeBuffer .= $data;
        }
        $bytesToWrite = strlen($this->writeBuffer);
        $bytesWritten = @fwrite($this->stream, $this->writeBuffer);

        if ($bytesToWrite === $bytesWritten) {
            \Amp\disable($this->writeWatcher);
        } elseif ($bytesWritten >= 0) {
            $this->writeBuffer = substr($client->writeBuffer, $bytesWritten);
            \Amp\enable($this->writeWatcher);
        } elseif ($this->isSocketDead($this->stream)) {
            $this->reconnet();
        }
    }

    // }}}
    // {{{ protected function isSocketDead()
    
    /**
     * check the stream is close
     *
     * @return bool
     */
    protected function isSocketDead() 
    {
        return !is_resource($socket) || @feof($socket);
    }

    // }}}
}
