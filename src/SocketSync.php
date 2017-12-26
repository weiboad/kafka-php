<?php
namespace Kafka;

class SocketSync extends CommonSocket
{
    public function connect(): void
    {
        if (is_resource($this->stream)) {
            return;
        }

        $this->createStream();

        stream_set_blocking($this->stream, 0);
    }

    public function close() : void
    {
        if (is_resource($this->stream)) {
            fclose($this->stream);
        }
    }

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
     * @throws \Kafka\Exception
     */
    public function read(int $length) : string
    {
        return $this->readBlocking($length);
    }

    /**
     * @throws \Kafka\Exception
     */
    public function write(string $buffer) : int
    {
        return $this->writeBlocking($buffer);
    }

    public function rewind()
    {
        if (is_resource($this->stream)) {
            rewind($this->stream);
        }
    }
}
