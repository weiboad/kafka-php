<?php
declare(strict_types=1);

namespace Kafka;

class SocketSync extends CommonSocket
{
    public function connect(): void
    {
        if (\is_resource($this->stream)) {
            return;
        }

        $this->createStream();

        \stream_set_blocking($this->stream, false);
    }

    public function close(): void
    {
        if (\is_resource($this->stream)) {
            \fclose($this->stream);
        }
    }

    public function isResource(): bool
    {
        return \is_resource($this->stream);
    }

    /**
     * @param string|int $data
     *
     * @throws \Kafka\Exception
     */
    public function read($data): string
    {
        return $this->readBlocking((int) $data);
    }

    /**
     * @throws \Kafka\Exception
     */
    public function write(?string $buffer = null): int
    {
        if ($buffer === null) {
            throw new Exception('You must inform some data to be written');
        }

        return $this->writeBlocking($buffer);
    }

    public function rewind(): void
    {
        if (\is_resource($this->stream)) {
            \rewind($this->stream);
        }
    }
}
