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
    }

    // }}}
    // }}}
}
