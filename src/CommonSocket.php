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
* Kafka broker socket abstract
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

abstract class CommonSocket
{
    // {{{ consts

    const READ_MAX_LENGTH = 5242880; // read socket max length 5MB

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
     * @access protected
     */
    protected $sendTimeoutSec = 0;

    /**
     * Send timeout in microseconds.
     *
     * @var float
     * @access protected
     */
    protected $sendTimeoutUsec = 100000;

    /**
     * Recv timeout in seconds
     *
     * @var float
     * @access protected
     */
    protected $recvTimeoutSec = 0;

    /**
     * Recv timeout in microseconds
     *
     * @var float
     * @access protected
     */
    protected $recvTimeoutUsec = 750000;

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
     * Max Write Attempts
     * @var int
     * @access protected
     */
    protected $maxWriteAttempts = 3;

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
    private $saslMechanismProvider = null;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    /**
     * __construct
     *
     * @access public
     * @param $host
     * @param $port
     * @param Object $config
     */
    public function __construct(string $host, int $port, ?Config $config = null, ?SaslMechanism $saslProvider = null)
    {
        $this->host                  = $host;
        $this->port                  = $port;
        $this->config                = $config;
        $this->saslMechanismProvider = $saslProvider;
    }

    // }}}
    // {{{ public function setSendTimeoutSec()

    /**
     * @param float $sendTimeoutSec
     */
    public function setSendTimeoutSec(float $sendTimeoutSec) : void
    {
        $this->sendTimeoutSec = $sendTimeoutSec;
    }

    // }}}
    // {{{ public function setSendTimeoutUsec()

    /**
     * @param float $sendTimeoutUsec
     */
    public function setSendTimeoutUsec(float $sendTimeoutUsec) : void
    {
        $this->sendTimeoutUsec = $sendTimeoutUsec;
    }

    // }}}
    // {{{ public function setRecvTimeoutSec()

    /**
     * @param float $recvTimeoutSec
     */
    public function setRecvTimeoutSec(float $recvTimeoutSec) : void
    {
        $this->recvTimeoutSec = $recvTimeoutSec;
    }
    
    // }}}
    // {{{ public function setRecvTimeoutUsec()

    /**
     * @param float $recvTimeoutUsec
     */
    public function setRecvTimeoutUsec(float $recvTimeoutUsec) : void
    {
        $this->recvTimeoutUsec = $recvTimeoutUsec;
    }
    
    // }}}
    // {{{ public function setMaxWriteAttempts()

    /**
     * @param int $number
     */
    public function setMaxWriteAttempts(int $number) : void
    {
        $this->maxWriteAttempts = $number;
    }

    // }}}
    // {{{ protected function createStream()

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
        if ($this->config != null && $this->config->getSslEnable()) { // ssl connection
            $remoteSocket = sprintf('ssl://%s:%s', $this->host, $this->port);
            $localCert    = $this->config->getSslLocalCert();
            $localKey     = $this->config->getSslLocalPk();
            $verifyPeer   = $this->config->getSslVerifyPeer();
            $passphrase   = $this->config->getSslPassphrase();
            $cafile       = $this->config->getSslCafile();
            $peerName     = $this->config->getSslPeerName();

            $context = stream_context_create(['ssl' => [
                'local_cert' => $localCert,
                'local_pk' => $localKey,
                'verify_peer' => $verifyPeer,
                'passphrase' => $passphrase,
                'cafile' => $cafile,
                'peer_name' => $peerName
            ]]);
        }
        
        $this->stream = $this->createSocket($remoteSocket, $context, $errno, $errstr);

        if ($this->stream == false) {
            $error = 'Could not connect to '
                    . $this->host . ':' . $this->port
                    . ' (' . $errstr . ' [' . $errno . '])';
            throw new \Kafka\Exception($error);
        }
        // SASL auth
        if ($this->saslMechanismProvider !== null) {
            $this->saslMechanismProvider->authenticate($this);
        }
    }

    // }}}
    // {{{ protected function createSocket()

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
            $this->sendTimeoutSec + ($this->sendTimeoutUsec / 1000000),
            STREAM_CLIENT_CONNECT,
            $context
        );
    }

    // }}}

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
    // {{{ public function readBlocking()

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
        $readable = $this->select($read, $this->recvTimeoutSec, $this->recvTimeoutUsec);
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
                $readable = $this->select($read, $this->recvTimeoutSec, $this->recvTimeoutUsec);
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

    // }}}
    // {{{ public function writeBlocking()

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
            $writable = $this->select($write, $this->sendTimeoutSec, $this->sendTimeoutUsec, false);
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
                if ($failedAttempts > $this->maxWriteAttempts) {
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

    // }}}
    // {{{ abstract public function close()

    /**
     * close the socket
     *
     * @access public
     * @return void
     */
    abstract public function close() : void;

    // }}}
    // }}}
}
