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

use Amp\Loop;

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

class Socket extends CommonSocket
{
    // {{{ members

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
    // {{{ public function connect()

    /**
     * Connects the socket
     *
     * @access public
     * @return void
     */
    public function connect()
    {
        if (! $this->isSocketDead()) {
            return;
        }

        $this->createStream();

        stream_set_blocking($this->getSocket(), 0);
        stream_set_read_buffer($this->getSocket(), 0);

        $this->readWatcher = Loop::onReadable($this->getSocket(), function () {
            do {
                if (! $this->isSocketDead()) {
                    $newData = @fread($this->getSocket(), self::READ_MAX_LEN);
                } else {
                    $this->reconnect();
                    return;
                }
                if ($newData) {
                    $this->read($newData);
                }
            } while ($newData);
        });

        $this->writeWatcher = Loop::onWritable($this->getSocket(), function () {
            $this->write();
        }, ['enable' => false]); // <-- let's initialize the watcher as "disabled"
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
    // {{{ public function setOnReadable()

    public $onReadable;
    /**
     * set on readable callback function
     *
     * @access public
     * @return void
     */
    public function setOnReadable(\Closure $read)
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
        Loop::cancel($this->readWatcher);
        Loop::cancel($this->writeWatcher);
        if (is_resource($this->getSocket())) {
            fclose($this->getSocket());
        }
        $this->readBuffer     = '';
        $this->writeBuffer    = '';
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
        return is_resource($this->getSocket());
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
                $dataLen              = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($this->readBuffer, 0, 4));
                $this->readNeedLength = $dataLen;
                $this->readBuffer     = substr($this->readBuffer, 4);
            }

            if (strlen($this->readBuffer) < $this->readNeedLength) {
                return;
            }
            $data = substr($this->readBuffer, 0, $this->readNeedLength);

            $this->readBuffer     = substr($this->readBuffer, $this->readNeedLength);
            $this->readNeedLength = 0;
            call_user_func($this->onReadable, $data, (int) $this->getSocket());
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
        $bytesWritten = @fwrite($this->getSocket(), $this->writeBuffer);

        if ($bytesToWrite === $bytesWritten) {
            Loop::disable($this->writeWatcher);
        } elseif ($bytesWritten >= 0) {
            Loop::enable($this->writeWatcher);
        } elseif ($this->isSocketDead()) {
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
        return ! is_resource($this->getSocket()) || @feof($this->getSocket());
    }

    // }}}
    // }}}
}
