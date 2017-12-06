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

    const READ_MAX_LEN = 5242880; // read socket max length 5MB

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
     * @access protected
     */
    protected $provider = null;

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
    public function __construct($host, $port, $config = null)
    {
        $this->host   = $host;
        $this->port   = $port;
        $this->config = $config;
    }

    /**
     * @param float $sendTimeoutSec
     */
    public function setSendTimeoutSec($sendTimeoutSec)
    {
        $this->sendTimeoutSec = $sendTimeoutSec;
    }

    /**
     * @param float $sendTimeoutUsec
     */
    public function setSendTimeoutUsec($sendTimeoutUsec)
    {
        $this->sendTimeoutUsec = $sendTimeoutUsec;
    }

    /**
     * @param float $recvTimeoutSec
     */
    public function setRecvTimeoutSec($recvTimeoutSec)
    {
        $this->recvTimeoutSec = $recvTimeoutSec;
    }

    /**
     * @param float $recvTimeoutUsec
     */
    public function setRecvTimeoutUsec($recvTimeoutUsec)
    {
        $this->recvTimeoutUsec = $recvTimeoutUsec;
    }

    /**
     * @param int $number
     */
    public function setMaxWriteAttempts($number)
    {
        $this->maxWriteAttempts = $number;
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
    // {{{ public function setSaslProvider()

    /**
     * set SASL provider
     *
     * @access public
     * @param SaslMechanism provider
     * @return void
     */
    public function setSaslProvider(SaslMechanism $provider)
    {
        $this->provider = $provider;
    }

    // }}}
    // {{{ protected function createStream()

    /**
     * create the socket stream
     *
     * @access public
     * @return void
     */
    protected function createStream()
    {
        if (empty($this->host)) {
            throw new \Kafka\Exception('Cannot open null host.');
        }
        if ($this->port <= 0) {
            throw new \Kafka\Exception('Cannot open without port.');
        }
        
        $remoteSocket = sprintf('tcp://%s:%s', $this->host, $this->port);
        $context      = stream_context_create([]);
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
        if ($this->provider != null) {
            $this->provider->authenticate($this);
        }
    }

    // }}}
    // {{{ public function selectRead()

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
    public function selectRead($len, $verifyExactLength = false)
    {
        if ($len > self::READ_MAX_LEN) {
            throw new \Kafka\Exception('Could not read ' . $len . ' bytes from stream, length too longer.');
        }

        $null     = null;
        $read     = [$this->getSocket()];
        $readable = @stream_select($read, $null, $null, $this->recvTimeoutSec, $this->recvTimeoutUsec);
        if ($readable > 0) {
            $remainingBytes = $len;
            $data           = $chunk = '';
            while ($remainingBytes > 0) {
                $chunk = fread($this->getSocket(), $remainingBytes);
                if ($chunk === false) {
                    $this->close();
                    throw new \Kafka\Exception('Could not read ' . $len . ' bytes from stream (no data)');
                }
                if (strlen($chunk) === 0) {
                    // Zero bytes because of EOF?
                    if (feof($this->getSocket())) {
                        $this->close();
                        throw new \Kafka\Exception('Unexpected EOF while reading ' . $len . ' bytes from stream (no data)');
                    }
                    // Otherwise wait for bytes
                    $readable = @stream_select($read, $null, $null, $this->recvTimeoutSec, $this->recvTimeoutUsec);
                    if ($readable !== 1) {
                        throw new \Kafka\Exception('Timed out reading socket while reading ' . $len . ' bytes with ' . $remainingBytes . ' bytes to go');
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
        if (false !== $readable) {
            $res = stream_get_meta_data($this->getSocket());
            if (! empty($res['timed_out'])) {
                $this->close();
                throw new \Kafka\Exception('Timed out reading ' . $len . ' bytes from stream');
            }
        }
        $this->close();
        throw new \Kafka\Exception('Could not read ' . $len . ' bytes from stream (not readable)');
    }

    // }}}
    // {{{ public function selectWrite()

    /**
     * Write to the socket.
     *
     * @param string $buf The data to write
     *
     * @return integer
     * @throws \Kafka\Exception
     */
    public function selectWrite($buf)
    {
        $null  = null;
        $write = [$this->getSocket()];

        // fwrite to a socket may be partial, so loop until we
        // are done with the entire buffer
        $failedWriteAttempts = 0;
        $written             = 0;
        $buflen              = strlen($buf);
        while ($written < $buflen) {
            // wait for stream to become available for writing
            $writable = stream_select($null, $write, $null, $this->sendTimeoutSec, $this->sendTimeoutUsec);
            if ($writable > 0) {
                if ($buflen - $written > self::MAX_WRITE_BUFFER) {
                    // write max buffer size
                    $wrote = fwrite($this->getSocket(), substr($buf, $written, self::MAX_WRITE_BUFFER));
                } else {
                    // write remaining buffer bytes to stream
                    $wrote = fwrite($this->getSocket(), substr($buf, $written));
                }
                if ($wrote === -1 || $wrote === false) {
                    throw new \Kafka\Exception\Socket('Could not write ' . strlen($buf) . ' bytes to stream, completed writing only ' . $written . ' bytes');
                } elseif ($wrote === 0) {
                    // Increment the number of times we have failed
                    $failedWriteAttempts++;
                    if ($failedWriteAttempts > $this->maxWriteAttempts) {
                        throw new \Kafka\Exception\Socket('After ' . $failedWriteAttempts . ' attempts could not write ' . strlen($buf) . ' bytes to stream, completed writing only ' . $written . ' bytes');
                    }
                } else {
                    // If we wrote something, reset our failed attempt counter
                    $failedWriteAttempts = 0;
                }
                $written += $wrote;
                continue;
            }
            if (false !== $writable) {
                $res = stream_get_meta_data($this->getSocket());
                if (! empty($res['timed_out'])) {
                    throw new \Kafka\Exception('Timed out writing ' . strlen($buf) . ' bytes to stream after writing ' . $written . ' bytes');
                }
            }
            throw new \Kafka\Exception\Socket('Could not write ' . strlen($buf) . ' bytes to stream');
        }
        return $written;
    }

    // }}}
    // {{{ abstract public function close()

    /**
     * close the socket
     *
     * @access public
     * @return void
     */
    abstract public function close();

    // }}}
    // }}}
}
