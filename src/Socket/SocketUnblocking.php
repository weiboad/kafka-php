<?php
namespace Kafka\Socket;

use Amp\Loop;

class SocketUnblocking extends Socket
{

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

    /**
     * Connects the socket
     *
     * @access public
     * @return void
     */
    protected function preformConnect() : void
    {
        if (! $this->isSocketDead()) {
            return;
        }

        $this->createStream();

        stream_set_blocking($this->stream, 0);
        stream_set_read_buffer($this->stream, 0);

        $this->readWatcher = Loop::onReadable($this->stream, function () {
            do {
                if (! $this->isSocketDead()) {
                    $newData = @fread($this->stream, self::READ_MAX_LENGTH);
                } else {
                    $this->reconnect();
                    return;
                }
                if ($newData) {
                    $this->read($newData);
                }
            } while ($newData);
        });

        $this->writeWatcher = Loop::onWritable($this->stream, function () {
            $this->write();
        }, ['enable' => false]); // <-- let's initialize the watcher as "disabled"
    }

    /**
     * reconnect the socket
     *
     * @access public
     * @return void
     */
    private function reconnect()
    {
        $this->close();
        $this->connect();
    }

    public $onReadable;

    /**
     * set on readable callback function
     *
     * @access public
     * @return void
     */
    public function setOnReadable(callable $read) : void
    {
        $this->onReadable = $read;
    }

    /**
     * close the socket
     *
     * @access public
     * @return void
     */
    public function close() : void
    {
        Loop::cancel($this->readWatcher);
        Loop::cancel($this->writeWatcher);
        if (is_resource($this->stream)) {
            fclose($this->stream);
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
        return is_resource($this->stream);
    }

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
            call_user_func($this->onReadable, $data, (int) $this->stream);
        } while (strlen($this->readBuffer));
    }

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
            Loop::disable($this->writeWatcher);
        } elseif ($bytesWritten >= 0) {
            Loop::enable($this->writeWatcher);
        } elseif ($this->isSocketDead()) {
            $this->reconnect();
        }
        $this->writeBuffer = substr($this->writeBuffer, $bytesWritten);
    }

	public function getSocket()
	{
		return $this->stream;
	}

    /**
     * check the stream is close
     *
     * @return bool
     */
    protected function isSocketDead()
    {
        return ! is_resource($this->stream) || @feof($this->stream);
    }
}
