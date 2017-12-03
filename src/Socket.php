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

namespace Kafka;

/**
+------------------------------------------------------------------------------
* Kafka broker socket
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Socket
{
    // {{{ consts

    const READ_MAX_LEN = 5242880; // read socket max length 5MB

    /**
     * max write socket buffer
     * fixed:send of 8192 bytes failed with errno=11 Resource temporarily
     * fixed:'fwrite(): send of ???? bytes failed with errno=35 Resource temporarily unavailable'
     * unavailable error info
     */
    const MAX_WRITE_BUFFER = 2048;

    // }}}
    // {{{ members

    /**
     * Send timeout in seconds.
     *
     * @var float
     * @access private
     */
    private $sendTimeoutSec = 0;

    /**
     * Send timeout in microseconds.
     *
     * @var float
     * @access private
     */
    private $sendTimeoutUsec = 100000;

    /**
     * Recv timeout in seconds
     *
     * @var float
     * @access private
     */
    private $recvTimeoutSec = 0;

    /**
     * Recv timeout in microseconds
     *
     * @var float
     * @access private
     */
    private $recvTimeoutUsec = 750000;

    /**
     * Stream resource
     *
     * @var mixed
     * @access private
     */
    private $stream = null;

    /**
     * Socket host
     *
     * @var mixed
     * @access private
     */
    private $host = null;

    /**
     * Socket port
     *
     * @var mixed
     * @access private
     */
    private $port = -1;

    /**
     * Max Write Attempts
     * @var int
     * @access private
     */
    private $maxWriteAttempts = 3;

    /**
     * Reader watcher
     * @var int
     * @access private
     */
    private $readWatcher = 0;

    /**
     * Write watcher
     * @var int
     * @access private
     */
    private $writeWatcher = 0;

    /**
     * Write watcher
     * @var int
     * @access private
     */
    private $writeBuffer = '';

    /**
     * Reader buffer
     * @var int
     * @access private
     */
    private $readBuffer = '';

    /**
     * Reader need buffer length
     * @var int
     * @access private
     */
    private $readNeedLength = 0;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    /**
     * __construct
     *
     * @access public
     * @param $host
     * @param $port
     * @param int $recvTimeoutSec
     * @param int $recvTimeoutUsec
     * @param int $sendTimeoutSec
     * @param int $sendTimeoutUsec
     */
    public function __construct($host, $port, $recvTimeoutSec = 0, $recvTimeoutUsec = 750000, $sendTimeoutSec = 0, $sendTimeoutUsec = 100000)
    {
        $this->host = $host;
        $this->port = $port;
        $this->setRecvTimeoutSec($recvTimeoutSec);
        $this->setRecvTimeoutUsec($recvTimeoutUsec);
        $this->setSendTimeoutSec($sendTimeoutSec);
        $this->setSendTimeoutUsec($sendTimeoutUsec);
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
    // {{{ public function connect()

    /**
     * Connects the socket
     *
     * @access public
     * @return void
     */
    public function connect()
    {
        if (!$this->isSocketDead()) {
            return;
        }

        if (empty($this->host)) {
            throw new \Kafka\Exception('Cannot open null host.');
        }
        if ($this->port <= 0) {
            throw new \Kafka\Exception('Cannot open without port.');
        }

        $this->stream = @fsockopen(
            $this->host,
            $this->port,
            $errno,
            $errstr,
            $this->sendTimeoutSec + ($this->sendTimeoutUsec / 1000000)
        );

        if ($this->stream == false) {
            $error = 'Could not connect to '
                    . $this->host . ':' . $this->port
                    . ' ('.$errstr.' ['.$errno.'])';
            throw new \Kafka\Exception($error);
        }

        stream_set_blocking($this->stream, 0);
        stream_set_read_buffer($this->stream, 0);

        $this->readWatcher = \Amp\onReadable($this->stream, function () {
            do {
                if(!$this->isSocketDead($this->stream)){
                    $newData = @fread($this->stream, self::READ_MAX_LEN);
                }else{
                    $this->reconnect();
                    return;
                }
                if ($newData) {
                    $this->read($newData);
                }
            } while ($newData);
        });

        $this->writeWatcher = \Amp\onWritable($this->stream, function () {
            $this->write();
        }, array('enable' => false)); // <-- let's initialize the watcher as "disabled"
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
    // {{{ public function getSocket()

    /**
     * get the socket
     *
     * @access public
     * @return void
     */
    public function getSocket()
    {
        return $this->stream;
    }

    // }}}
    // {{{ public function SetOnReadable()

    public $onReadable;
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
        do {
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
            $data = substr($this->readBuffer, 0, $this->readNeedLength);

            $this->readBuffer = substr($this->readBuffer, $this->readNeedLength);
            $this->readNeedLength = 0;
            call_user_func($this->onReadable, $data, (int)$this->stream);
        } while (strlen($this->readBuffer));
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
            \Amp\enable($this->writeWatcher);
        } elseif ($this->isSocketDead($this->stream)) {
            $this->reconnect();
        }
        $this->writeBuffer = substr($this->writeBuffer, $bytesWritten);
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
        return !is_resource($this->stream) || @feof($this->stream);
    }

    // }}}
    // }}}
}
