<?php
declare(strict_types=1);

namespace Kafka;

use const STREAM_CLIENT_CONNECT;
use function feof;
use function fread;
use function fwrite;
use function is_resource;
use function sprintf;
use function stream_context_create;
use function stream_get_meta_data;
use function stream_select;
use function stream_socket_client;
use function strlen;
use function substr;
use function trim;

abstract class CommonSocket
{
    public const READ_MAX_LENGTH = 5242880; // read socket max length 5MB

    /**
     * max write socket buffer
     * fixed:send of 8192 bytes failed with errno=11 Resource temporarily
     * fixed:'fwrite(): send of ???? bytes failed with errno=35 Resource temporarily unavailable'
     * unavailable error info
     */
    public const MAX_WRITE_BUFFER = 2048;

    /**
     * @var resource
     */
    protected $stream;

    /**
     * @var string|null
     */
    protected $host;

    /**
     * @var int
     */
    protected $port = -1;

    /**
     * @var int
     */
    protected $maxWriteAttempts = 3;

    /**
     * @var Config|null
     */
    protected $config;

    /**
     * @var SaslMechanism|null
     */
    private $saslProvider;

    public function __construct(string $host, int $port, ?Config $config = null, ?SaslMechanism $saslProvider = null)
    {
        $this->host         = $host;
        $this->port         = $port;
        $this->config       = $config;
        $this->saslProvider = $saslProvider;
    }

    public function setMaxWriteAttempts(int $number): void
    {
        $this->maxWriteAttempts = $number;
    }

    /**
     * @throws Exception
     */
    protected function createStream(): void
    {
        if (trim($this->host) === '') {
            throw new Exception('Cannot open null host.');
        }

        if ($this->port <= 0) {
            throw new Exception('Cannot open without port.');
        }

        $remoteSocket = sprintf('tcp://%s:%s', $this->host, $this->port);
        $context      = stream_context_create([]);

        if ($this->config !== null && $this->config->getSslEnable()) { // ssl connection
            $remoteSocket = sprintf('ssl://%s:%s', $this->host, $this->port);

            $filterFunction = function ($elem) {
                return $elem !== '';
            };
            $context = stream_context_create(
                [
                    'ssl' => array_filter([
                        'local_cert'       => $this->config->getSslLocalCert(),
                        'local_pk'         => $this->config->getSslLocalPk(),
                        'verify_peer'      => true,
                        'verify_peer_name' => false,
                        'passphrase'       => $this->config->getSslPassphrase(),
                        'cafile'           => $this->config->getSslCafile(),
                        'peer_name'        => $this->config->getSslPeerName(),
                    ], $filterFunction),
                ]
            );
        }

        $this->stream = $this->createSocket($remoteSocket, $context, $errno, $errstr);

        if (! is_resource($this->stream)) {
            throw new Exception(
                sprintf('Could not connect to %s:%d (%s [%d])', $this->host, $this->port, $errstr, $errno)
            );
        }

        // SASL auth
        if ($this->saslProvider !== null) {
            $this->saslProvider->authenticate($this);
        }
    }

    /**
     * Encapsulation of stream_socket_client
     *
     * Because `stream_socket_client` in stream wrapper mock no effect, if don't create this function will never be testable
     *
     * @codeCoverageIgnore
     *
     * @param resource $context
     *
     * @return resource
     */
    protected function createSocket(string $remoteSocket, $context, ?int &$errno, ?string &$errstr)
    {
        return stream_socket_client(
            $remoteSocket,
            $errno,
            $errstr,
            $this->config->getSendTimeoutSec() + ($this->config->getSendTimeoutUsec() / 1000000),
            STREAM_CLIENT_CONNECT,
            $context
        );
    }

    /**
     * @return resource
     */
    public function getSocket()
    {
        return $this->stream;
    }

    /**
     * Encapsulation of stream_select
     *
     * Because `stream_select` in stream wrapper mock no effect, if don't create this function will never be testable
     *
     * @codeCoverageIgnore
     *
     * @param resource[] $sockets
     *
     * @return int|bool
     */
    protected function select(array $sockets, int $timeoutSec, int $timeoutUsec, bool $isRead = true)
    {
        $null = null;

        if ($isRead) {
            return @stream_select($sockets, $null, $null, $timeoutSec, $timeoutUsec);
        }

        return @stream_select($null, $sockets, $null, $timeoutSec, $timeoutUsec);
    }

    /**
     * Encapsulation of stream_get_meta_data
     *
     * Because `stream_get_meta_data` in stream wrapper mock no effect, if don't create this function will never be testable
     *
     * @codeCoverageIgnore
     *
     * @return mixed[]
     */
    protected function getMetaData(): array
    {
        return stream_get_meta_data($this->stream);
    }

    /**
     * Read from the socket at most $len bytes.
     *
     * This method will not wait for all the requested data, it will return as
     * soon as any data is received.
     *
     * @throws Exception
     */
    public function readBlocking(int $length): string
    {
        if ($length > self::READ_MAX_LENGTH) {
            throw Exception\Socket::invalidLength($length, self::READ_MAX_LENGTH);
        }

        $readable = $this->select([$this->stream], $this->config->getRecvTimeoutSec(), $this->config->getRecvTimeoutUsec());

        if ($readable === false) {
            $this->close();
            throw Exception\Socket::notReadable($length);
        }

        if ($readable === 0) { // select timeout
            $res = $this->getMetaData();
            $this->close();

            if (! empty($res['timed_out'])) {
                throw Exception\Socket::timedOut($length);
            }

            throw Exception\Socket::notReadable($length);
        }

        $remainingBytes = $length;
        $data           = $chunk = '';

        while ($remainingBytes > 0) {
            $chunk = fread($this->stream, $remainingBytes);

            if ($chunk === false || strlen($chunk) === 0) {
                // Zero bytes because of EOF?
                if (feof($this->stream)) {
                    $this->close();
                    throw Exception\Socket::unexpectedEOF($length);
                }
                // Otherwise wait for bytes
                $readable = $this->select([$this->stream], $this->config->getRecvTimeoutSec(), $this->config->getRecvTimeoutUsec());
                if ($readable !== 1) {
                    throw Exception\Socket::timedOutWithRemainingBytes($length, $remainingBytes);
                }

                continue; // attempt another read
            }

            $data           .= $chunk;
            $remainingBytes -= strlen($chunk);
        }

        return $data;
    }

    /**
     * Write to the socket.
     *
     * @throws Exception
     */
    public function writeBlocking(string $buffer): int
    {
        // fwrite to a socket may be partial, so loop until we
        // are done with the entire buffer
        $failedAttempts = 0;
        $bytesWritten   = 0;

        $bytesToWrite = strlen($buffer);

        while ($bytesWritten < $bytesToWrite) {
            // wait for stream to become available for writing
            $writable = $this->select([$this->stream], $this->config->getSendTimeoutSec(), $this->config->getSendTimeoutUsec(), false);

            if ($writable === false) {
                throw new Exception\Socket('Could not write ' . $bytesToWrite . ' bytes to stream');
            }

            if ($writable === 0) {
                $res = $this->getMetaData();
                if (! empty($res['timed_out'])) {
                    throw new Exception('Timed out writing ' . $bytesToWrite . ' bytes to stream after writing ' . $bytesWritten . ' bytes');
                }

                throw new Exception\Socket('Could not write ' . $bytesToWrite . ' bytes to stream');
            }

            if ($bytesToWrite - $bytesWritten > self::MAX_WRITE_BUFFER) {
                // write max buffer size
                $wrote = fwrite($this->stream, substr($buffer, $bytesWritten, self::MAX_WRITE_BUFFER));
            } else {
                // write remaining buffer bytes to stream
                $wrote = fwrite($this->stream, substr($buffer, $bytesWritten));
            }

            if ($wrote === -1 || $wrote === false) {
                throw new Exception\Socket('Could not write ' . strlen($buffer) . ' bytes to stream, completed writing only ' . $bytesWritten . ' bytes');
            }

            if ($wrote === 0) {
                // Increment the number of times we have failed
                $failedAttempts++;

                if ($failedAttempts > $this->maxWriteAttempts) {
                    throw new Exception\Socket('After ' . $failedAttempts . ' attempts could not write ' . strlen($buffer) . ' bytes to stream, completed writing only ' . $bytesWritten . ' bytes');
                }
            } else {
                // If we wrote something, reset our failed attempt counter
                $failedAttempts = 0;
            }

            $bytesWritten += $wrote;
        }

        return $bytesWritten;
    }

    abstract public function close(): void;

    abstract public function connect(): void;

    /**
     * @return void|int
     */
    abstract public function write(?string $data = null);

    /**
     * @param string|int $data
     *
     * @return mixed
     */
    abstract public function read($data);
}
