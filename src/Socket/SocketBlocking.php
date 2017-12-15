<?php
namespace Kafka\Socket;

use Kafka\Socket\Socket;

class SocketBlocking extends Socket
{
    /**
     * Read from the socket at most $len bytes.
     *
     * This method will not wait for all the requested data, it will return as
     * soon as any data is received.
     *
     * @param integer $len               Maximum number of bytes to read.
     *
     * @return string Binary data
     * @throws \Kafka\Exception
     */
    public function read($len)
    {
        return $this->readBlocking($len);
    }

    /**
     * Write to the socket.
     *
     * @param string $buf The data to write
     *
     * @return integer
     * @throws \Kafka\Exception
     */
    public function write($buf)
    {
        return $this->writeBlocking($buf);
    }

    public function setOnReadable(?callable $read) : void
    {
    }

    /**
     * Connects the socket
     *
     * @access public
     * @return void
     */
    protected function preformConnect() : void
    {
        if (is_resource($this->stream)) {
            return;
        }

        $this->createStream();

        stream_set_blocking($this->stream, 0);
    }

    /**
     * close the socket
     *
     * @access public
     * @return void
     */
    public function close() : void
    {
        if (is_resource($this->stream)) {
            fclose($this->stream);
        }
    }
}
