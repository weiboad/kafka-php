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
    // {{{ public function getHost()
    
    public function getHost() : string
    {
        return $this->host;
    }

    // }}}
    // {{{ public function getPort()
    
    public function getPort() : int
    {
        return $this->port;
    }

    // }}}
    // {{{ public function setConfig()
    
    public function setConfig(Config $config) : void
    {
        $this->config = $config;
    }

    // }}}
    // {{{ public function getConfig()
    
    public function getConfig() : ?Config
    {
        return $this->config;
    }

    // }}}
    // {{{ public function getSaslProvider()
    
    public function getSaslProvider() : ?SaslMechanism
    {
        return $this->saslMechanismProvider;
    }

    // }}}
    // {{{ public function setSaslProvider()

    /**
     * set SASL provider
     *
     * @access public
     * @param SaslMechanism provider
     * @return void
     */
    public function setSaslProvider(SaslMechanism $provider) : void
    {
        $this->saslMechanismProvider = $provider;
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
    // {{{ public function getSendTimeoutSec()

    public function getSendTimeoutSec() : float
    {
        return $this->sendTimeoutSec;
    }

    // }}}
    // {{{ public function getSendTimeoutUsec()

    public function getSendTimeoutUsec() : float
    {
        return $this->sendTimeoutUsec;
    }

    // }}}
    // {{{ public function getRecvTimeoutSec()

    public function getRecvTimeoutSec() : float
    {
        return $this->recvTimeoutSec;
    }
    
    // }}}
    // {{{ public function getRecvTimeoutUsec()

    public function getRecvTimeoutUsec() : float
    {
        return $this->recvTimeoutUsec;
    }
    
    // }}}
    // {{{ public function getMaxWriteAttempts()

    /**
     * @param int $number
     */
    public function getMaxWriteAttempts() : int
    {
        return $this->maxWriteAttempts;
    }

    // }}}
    // {{{ public function getSocket()

    /**
     * get the socket
     *
     * @access public
     * @return resource
     */
    public function getSocket()
    {
        return $this->stream;
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
        
        $this->stream = stream_socket_client(
            $remoteSocket,
            $errno,
            $errstr,
            $this->sendTimeoutSec + ($this->sendTimeoutUsec / 1000000),
            STREAM_CLIENT_CONNECT,
            $context
        );

        if ($this->stream == false) {
            $error = 'Could not connect to '
                    . $this->host . ':' . $this->port
                    . ' (' . $errstr . ' [' . $errno . '])';
            throw new \Kafka\Exception($error);
        }
        // SASL auth
        if ($this->saslMechanismProvider != null) {
            $this->saslMechanismProvider->authenticate($this);
        }
    }

    // }}}
    // {{{ public function readBlocking()

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
     * @throws \Kafka\Exception
     */
    public function readBlocking(int $len, bool $verifyExactLength = false) : string
    {
        if ($len > self::READ_MAX_LENGTH) {
            throw new \Kafka\Exception('Invalid length given, it should be lesser than or equals to ' . self:: READ_MAX_LENGTH);
        }

        $null     = null;
        $read     = [$this->getSocket()];
        $readable = @stream_select($read, $null, $null, $this->recvTimeoutSec, $this->recvTimeoutUsec);
        if ($readable === false) {
            $this->close();
            throw new \Kafka\Exception('Could not read ' . $len . ' bytes from stream (not readable)');
        }
        if ($readable === 0) { // select timeout
            $res = stream_get_meta_data($this->getSocket());
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
            $chunk = fread($this->getSocket(), $remainingBytes);
            if ($chunk === false || strlen($chunk) === 0) {
                // Zero bytes because of EOF?
                if (feof($this->getSocket())) {
                    $this->close();
                    throw new \Kafka\Exception('Unexpected EOF while reading ' . $len . ' bytes from stream (no data)');
                }
                // Otherwise wait for bytes
                $readable = @stream_select($read, $null, $null, $this->recvTimeoutSec, $this->recvTimeoutUsec);
                if ($readable !== 1) {
                    throw new \Kafka\Exception('Timed out while reading ' . $len . ' bytes from socket, ' . $remainingBytes . ' bytes are still needed');
                }
                continue; // attempt another read
            }
            $data           .= $chunk;
            $remainingBytes -= strlen($chunk);
        }
        if ($len === $remainingBytes || ($verifyExactLength && $len !== strlen($data))) {
            // couldn't read anything at all OR reached EOF sooner than expected
            $this->close();
            throw new \Kafka\Exception('Read ' . strlen($data) . ' bytes instead of the requested ' . $len . ' bytes');
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
        $write = [$this->getSocket()];

        $null = null;
        // fwrite to a socket may be partial, so loop until we
        // are done with the entire buffer
        $failedAttempts = 0;
        $bytesWritten   = 0;
        $bytesToWrite   = strlen($buf);
        while ($bytesWritten < $bytesToWrite) {
            // wait for stream to become available for writing
            $writable = stream_select($null, $write, $null, $this->sendTimeoutSec, $this->sendTimeoutUsec);
            if (false === $writable) {
                throw new \Kafka\Exception\Socket('Could not write ' . $bytesToWrite . ' bytes to stream');
            }
            if (0 === $writable) {
                $res = stream_get_meta_data($this->getSocket());
                if (! empty($res['timed_out'])) {
                    throw new \Kafka\Exception('Timed out writing ' . $bytesToWrite . ' bytes to stream after writing ' . $bytesWritten . ' bytes');
                } else {
                    throw new \Kafka\Exception\Socket('Could not write ' . $bytesToWrite . ' bytes to stream');
                }
            }
            
            if ($bytesToWrite - $bytesWritten > self::MAX_WRITE_BUFFER) {
                // write max buffer size
                $wrote = fwrite($this->getSocket(), substr($buf, $bytesWritten, self::MAX_WRITE_BUFFER));
            } else {
                // write remaining buffer bytes to stream
                $wrote = fwrite($this->getSocket(), substr($buf, $bytesWritten));
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
