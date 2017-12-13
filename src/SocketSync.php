<?php
namespace Kafka;

class SocketSync extends CommonSocket
{

    /**
     * Optional method to set the internal stream handle
     *
     * @static
     * @access public
     * @param $stream
     * @return Socket
     */
    public static function createFromStream($stream)
    {
        $socket = new self('localhost', 0);
        $socket->setStream($stream);
        return $socket;
    }

    /**
     * Connects the socket
     *
     * @access public
     * @return void
     */
    public function connect()
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
     *
     * @return string Binary data
     * @throws \Kafka\Exception
     */
    public function read(int $len) : string
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
    public function write(string $buf) : int
    {
        return $this->writeBlocking($buf);
    }

    /**
     * Rewind the stream
     *
     * @return void
     */
    public function rewind()
    {
        if (is_resource($this->stream)) {
            rewind($this->stream);
        }
    }
}
