<?php
declare(strict_types=1);

namespace Kafka;

use Amp\Loop;
use Kafka\Protocol\Protocol;

class Socket extends CommonSocket
{
    /**
     * @var string
     */
    private $readWatcherId = '';

    /**
     * @var string
     */
    private $writeWatcherId = '';

    /**
     * @var string
     */
    private $writeBuffer = '';

    /**
     * @var string
     */
    private $readBuffer = '';

    /**
     * @var int
     */
    private $readNeedLength = 0;

    /**
     * @var callable|null
     */
    private $onReadable;

    public function connect(): void
    {
        if (! $this->isSocketDead()) {
            return;
        }

        $this->createStream();

        \stream_set_blocking($this->stream, false);
        \stream_set_read_buffer($this->stream, 0);

        $this->readWatcherId = Loop::onReadable(
            $this->stream,
            function (): void {
                do {
                    if ($this->isSocketDead()) {
                        $this->reconnect();
                        return;
                    }

                    $newData = @\fread($this->stream, self::READ_MAX_LENGTH);

                    if ($newData) {
                        $this->read($newData);
                    }
                } while ($newData);
            }
        );

        $this->writeWatcherId = Loop::onWritable(
            $this->stream,
            function (): void {
                $this->write();
            },
            ['enable' => false] // <-- let's initialize the watcher as "disabled"
        );
    }

    public function reconnect(): void
    {
        $this->close();
        $this->connect();
    }

    public function setOnReadable(callable $read): void
    {
        $this->onReadable = $read;
    }

    public function close(): void
    {
        Loop::cancel($this->readWatcherId);
        Loop::cancel($this->writeWatcherId);

        if (\is_resource($this->stream)) {
            \fclose($this->stream);
        }

        $this->readBuffer     = '';
        $this->writeBuffer    = '';
        $this->readNeedLength = 0;
    }

    public function isResource(): bool
    {
        return \is_resource($this->stream);
    }

    /**
     * Read from the socket at most $len bytes.
     *
     * This method will not wait for all the requested data, it will return as
     * soon as any data is received.
     *
     * @param string|int $data
     */
    public function read($data): void
    {
        $this->readBuffer .= (string) $data;

        do {
            if ($this->readNeedLength === 0) { // response start
                if (\strlen($this->readBuffer) < 4) {
                    return;
                }

                $dataLen              = Protocol::unpack(Protocol::BIT_B32, \substr($this->readBuffer, 0, 4));
                $this->readNeedLength = $dataLen;
                $this->readBuffer     = \substr($this->readBuffer, 4);
            }

            if (\strlen($this->readBuffer) < $this->readNeedLength) {
                return;
            }

            $data = (string) \substr($this->readBuffer, 0, $this->readNeedLength);

            $this->readBuffer     = \substr($this->readBuffer, $this->readNeedLength);
            $this->readNeedLength = 0;

            ($this->onReadable)($data, (int) $this->stream);
        } while (\strlen($this->readBuffer));
    }

    /**
     * Write to the socket.
     *
     * @throws Loop\InvalidWatcherError
     */
    public function write(?string $data = null): void
    {
        if ($data !== null) {
            $this->writeBuffer .= $data;
        }

        $bytesToWrite = \strlen($this->writeBuffer);
        $bytesWritten = @\fwrite($this->stream, $this->writeBuffer);

        if ($bytesToWrite === $bytesWritten) {
            Loop::disable($this->writeWatcherId);
        } elseif ($bytesWritten >= 0) {
            Loop::enable($this->writeWatcherId);
        } elseif ($this->isSocketDead()) {
            $this->reconnect();
        }

        $this->writeBuffer = \substr($this->writeBuffer, $bytesWritten);
    }

    /**
     * check the stream is close
     */
    protected function isSocketDead(): bool
    {
        return ! \is_resource($this->stream) || @\feof($this->stream);
    }
}
