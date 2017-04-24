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

namespace Kafka\Consumer;

/**
+------------------------------------------------------------------------------
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Connection
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    // {{{ consts
    // }}}
    // {{{ members
    
    private static $instance = null;

    private $metaSockets = array();
    private $dataSockets = array();

    private $process;

    private $brokerMap;

    // }}}
    // {{{ functions
    // {{{ public function static getInstance()

    /**
     * set send messages
     *
     * @access public
     * @param $hostList
     * @param null $timeout
     * @return Consumer
     */
    public static function getInstance()
    {
        if (is_null(self::$instance)) {
            self::$instance = new self();
        }

        return self::$instance;
    }

    // }}}
    // {{{ private function __construct()

    /**
     * __construct
     *
     * @access public
     * @param $hostList
     * @param null $timeout
     */
    private function __construct()
    {
    }

    // }}}
    // {{{ public function setProcess()

    public function setProcess(\Closure $process) {
        $this->process = $process; 
    }

    // }}}
    // {{{ public function setBrokers()

    public function setBrokers($brokers) {
        $this->brokerMap = $brokers;
        foreach ($this->metaSockets as $key => $socket) {
            if (!isset($this->brokerMap[$key])) {
                $socket->close();
                unset($this->metaSockets[$key]);
            }
        }
        foreach ($this->dataSockets as $key => $socket) {
            if (!isset($this->brokerMap[$key])) {
                $socket->close();
                unset($this->dataSockets[$key]);
            }
        }
    }

    // }}}
    // {{{ public function setBrokers()

    public function getBrokers() {
        return $this->brokerMap;
    }

    // }}}
    // {{{ public function getMetaConnect()  

    public function getMetaConnect($key)
    { 
        return $this->getConnect($key, 'metaSockets');
    }

    // }}}
    // {{{ public function getRandConnect()  

    public function getRandConnect()
    { 
        $nodeIds = array_keys($this->brokerMap);
        shuffle($nodeIds);
        if (!isset($nodeIds[0])) {
            return false; 
        }
        return $this->getMetaConnect($nodeIds[0]);
    }

    // }}}
    // {{{ public function getDataConnect()  
    
    public function getDataConnect($key)
    { 
        return $this->getConnect($key, 'dataSockets');
    }

    // }}}
    // {{{ public function getConnect()  

    public function getConnect($key, $type)
    { 
        if (isset($this->{$type}[$key])) {
            return $this->{$type}[$key];
        }

        if (isset($this->brokerMap[$key])) {
            $hostname = $this->brokerMap[$key];
            if (isset($this->{$type}[$hostname])) {
                return $this->{$type}[$hostname];
            }
        }

        $host = null;
        $port = null;
        if (isset($this->brokerMap[$key])) {
            $hostname = $this->brokerMap[$key];
            list($host, $port) = explode(':', $hostname);
        }

        if (strpos($key, ':') !== false) {
            list($host, $port) = explode(':', $key);
        }

        if ($host && $port) {
            try {
                $socket = new \Kafka\SocketAsyn($host, $port);
                $socket->SetonReadable($this->process);
                $socket->connect();
                $this->{$type}[$key] = $socket;
                return $socket;
            } catch (\Exception $e) {
                $this->error($e->getMessage());
            }
        } else {
            return false;
        }
    }

    // }}}
    // }}}
}
