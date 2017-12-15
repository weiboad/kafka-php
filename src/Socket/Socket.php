<?php
namespace Kafka\Socket;

use Kafka\Contracts\SocketInterface;
use Kafka\Contracts\SaslMechanism;
use Kafka\Contracts\Config\Socket as SocketConfigInterface;
use Kafka\Contracts\Config\Ssl as SslConfigInterface;

abstract class Socket implements SocketInterface
{
    protected const READ_MAX_LENGTH = 5242880; // read socket max length 5MB

    /**
     * max write socket buffer
     * fixed:send of 8192 bytes failed with errno=11 Resource temporarily
     * fixed:'fwrite(): send of ???? bytes failed with errno=35 Resource temporarily unavailable'
     * unavailable error info
     */
    protected const MAX_WRITE_BUFFER = 2048;

    /**
     * Stream resource
     *
     * @var mixed
     * @access protected
     */
    protected $stream = null;

    /**
     * Socket host
     *
     * @var mixed
     * @access protected
     */
    protected $host = null;

    /**
     * Socket port
     *
     * @var mixed
     * @access protected
     */
    protected $port = -1;

    /**
     * Config socket connect params
     * @var Object
     * @access protected
     */
    protected $config = null;

    /**
     * Sasl provider
     * @var SaslMechanism
     * @access private
     */
    private $saslMechanismProvider;

	private $sslConfig;

    /**
     * __construct
     *
     * @access public
     * @param $host
     * @param $port
     * @param Object $config
     */
	public function __construct(string $host, int $port, SocketConfigInterface $config, 
		SaslMechanism $saslProvider,
		SslConfigInterface $sslConfig)
    {
        $this->host                  = $host;
        $this->port                  = $port;
        $this->config                = $config;
        $this->sslConfig             = $sslConfig;
        $this->saslMechanismProvider = $saslProvider;
    }

	public function connect() : void
	{
		$this->preformConnect();	
	}


    /**
     * create the socket stream
     *
     * @access public
     * @return void
     */
    protected function createStream() : void
    {
        if (empty($this->host)) {
            throw new \Kafka\Exception('Cannot open null host.');
        }
        if ($this->port <= 0) {
            throw new \Kafka\Exception('Cannot open without port.');
        }

        $remoteSocket = sprintf('tcp://%s:%s', $this->host, $this->port);

        $context = stream_context_create([]);
        if ($this->sslConfig->getEnable()) { // ssl connection
            $remoteSocket = sprintf('ssl://%s:%s', $this->host, $this->port);

            $context = stream_context_create(['ssl' => [
                'local_cert' => $this->sslConfig->getLocalCert(),
                'local_pk' => $this->sslConfig->getLocalPk(),
                'verify_peer' => $this->sslConfig->getVerifyPeer(),
                'passphrase' => $this->sslConfig->getPassphrase(),
                'cafile' => $this->sslConfig->getCafile(),
                'peer_name' => $this->sslConfig->getPeerName()
            ]]);
        }
        
        $this->stream = $this->createSocket($remoteSocket, $context, $errno, $errstr);

        if ($this->stream == false) {
            $error = 'Could not connect to '
                    . $this->host . ':' . $this->port
                    . ' (' . $errstr . ' [' . $errno . '])';
            throw new \Kafka\Exception($error);
        }
        $this->saslMechanismProvider->authenticate($this);
    }

    /**
     * Encapsulation of stream_socket_client
     *
     * Because `stream_socket_client` in stream wrapper mock no effect, if don't create this function will never be testable
     *
     * @codeCoverageIgnore
     */
    protected function createSocket($remoteSocket, $context, &$errno, &$errstr)
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
     * Encapsulation of stream_select
     *
     * Because `stream_select` in stream wrapper mock no effect, if don't create this function will never be testable
     *
     * @codeCoverageIgnore
     */
    protected function select($sockets, $timeoutSec, $timeoutUsec, $isRead = true)
    {
        $null = null;
        if ($isRead) {
            return @stream_select($sockets, $null, $null, $timeoutSec, $timeoutUsec);
        } else {
            return @stream_select($null, $sockets, $null, $timeoutSec, $timeoutUsec);
        }
    }

    /**
     * Encapsulation of stream_get_meta_data
     *
     * Because `stream_get_meta_data` in stream wrapper mock no effect, if don't create this function will never be testable
     *
     * @codeCoverageIgnore
     */
    protected function getMetaData()
    {
        return stream_get_meta_data($this->stream);
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
    public function readBlocking(int $len) : string
    {
        if ($len > self::READ_MAX_LENGTH) {
            throw new \Kafka\Exception('Invalid length given, it should be lesser than or equals to ' . self:: READ_MAX_LENGTH);
        }

        $null     = null;
        $read     = [$this->stream];
        $readable = $this->select($read, $this->config->getRecvTimeoutSec(), $this->config->getRecvTimeoutUsec());
        if ($readable === false) {
            $this->close();
            throw new \Kafka\Exception('Could not read ' . $len . ' bytes from stream (not readable)');
        }
        if ($readable === 0) { // select timeout
            $res = $this->getMetaData();
            $this->close();
            if (! empty($res['timed_out'])) {
                throw new \Kafka\Exception('Timed out reading ' . $len . ' bytes from stream');
            } else {
                throw new \Kafka\Exception('Could not read ' . $len . ' bytes from stream (not readable)');
            }
        }

        $remainingBytes = $len;
        $data           = $chunk = '';
        while ($remainingBytes > 0) {
            $chunk = fread($this->stream, $remainingBytes);
            if ($chunk === false || strlen($chunk) === 0) {
                // Zero bytes because of EOF?
                if (feof($this->stream)) {
                    $this->close();
                    throw new \Kafka\Exception('Unexpected EOF while reading ' . $len . ' bytes from stream (no data)');
                }
                // Otherwise wait for bytes
				$readable = $this->select($read, $this->config->getRecvTimeoutSec(), 
										  $this->config->getRecvTimeoutUsec());
                if ($readable !== 1) {
                    throw new \Kafka\Exception('Timed out while reading ' . $len . ' bytes from socket, ' . $remainingBytes . ' bytes are still needed');
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
     * @param string $buf The data to write
     *
     * @return integer
     * @throws \Kafka\Exception
     */
    public function writeBlocking(string $buf) : int
    {
        $write = [$this->stream];

        $null = null;
        // fwrite to a socket may be partial, so loop until we
        // are done with the entire buffer
        $failedAttempts = 0;
        $bytesWritten   = 0;
        $bytesToWrite   = strlen($buf);
        while ($bytesWritten < $bytesToWrite) {
            // wait for stream to become available for writing
			$writable = $this->select($write, $this->config->getSendTimeoutSec(),
									  $this->config->getSendTimeoutUsec(), false);
            if (false === $writable) {
                throw new \Kafka\Exception\Socket('Could not write ' . $bytesToWrite . ' bytes to stream');
            }
            if (0 === $writable) {
                $res = $this->getMetaData();
                if (! empty($res['timed_out'])) {
                    throw new \Kafka\Exception('Timed out writing ' . $bytesToWrite . ' bytes to stream after writing ' . $bytesWritten . ' bytes');
                } else {
                    throw new \Kafka\Exception\Socket('Could not write ' . $bytesToWrite . ' bytes to stream');
                }
            }
            
            if ($bytesToWrite - $bytesWritten > self::MAX_WRITE_BUFFER) {
                // write max buffer size
                $wrote = fwrite($this->stream, substr($buf, $bytesWritten, self::MAX_WRITE_BUFFER));
            } else {
                // write remaining buffer bytes to stream
                $wrote = fwrite($this->stream, substr($buf, $bytesWritten));
            }
            
            if ($wrote === -1 || $wrote === false) {
                throw new \Kafka\Exception\Socket('Could not write ' . strlen($buf) . ' bytes to stream, completed writing only ' . $bytesWritten . ' bytes');
            } elseif ($wrote === 0) {
                // Increment the number of times we have failed
                $failedAttempts++;
                if ($failedAttempts > $this->config->getMaxWriteAttempts()) {
                    throw new \Kafka\Exception\Socket('After ' . $failedAttempts . ' attempts could not write ' . strlen($buf) . ' bytes to stream, completed writing only ' . $bytesWritten . ' bytes');
                }
            } else {
                // If we wrote something, reset our failed attempt counter
                $failedAttempts = 0;
            }
            $bytesWritten += $wrote;
        }
        return $bytesWritten;
    }

    /**
     * close the socket
     *
     * @access public
     * @return void
     */
    abstract public function close() : void;
	abstract protected function preformConnect() : void;
}
