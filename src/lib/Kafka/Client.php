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
* Kafka protocol since Kafka v0.8 
+------------------------------------------------------------------------------
* 
* @package 
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$ 
+------------------------------------------------------------------------------
*/

class Client
{
    // {{{ consts
    // }}}
    // {{{ members

    /**
     * zookeeper 
     * 
     * @var mixed
     * @access private
     */
    private $zookeeper = null;

    /**
     * broker host list 
     * 
     * @var array
     * @access private
     */
    private $hostList = array();

    /**
     * save broker connection 
     * 
     * @var array
     * @access private
     */
    private static $stream = array();

    // }}} 
    // {{{ functions
    // {{{ public function __construct()

    /**
     * __construct 
     * 
     * @access public
     * @return void
     */
    public function __construct(\Kafka\ZooKeeper $zookeeper)
    {
        $this->zookeeper = $zookeeper;
    }

    // }}}
    // {{{ public function getBrokers()

    /**
     * get broker server 
     * 
     * @access public
     * @return void
     */
    public function getBrokers()
    { 
        if (empty($this->hostList)) {
            $brokerList = $this->zookeeper->listBrokers();
            foreach ($brokerList as $brokerId => $info) {
                if (!isset($info['host']) || !isset($info['port'])) {
                    continue;    
                }
                $this->hostList[$brokerId] = $info['host'] . ':' . $info['port'];
            }
        }

        return $this->hostList;
    }

    // }}}
    // {{{ public function getHostByPartition()

    /**
     * get broker host by topic partition 
     * 
     * @param string $topicName 
     * @param int $partitionId 
     * @access public
     * @return string
     */
    public function getHostByPartition($topicName, $partitionId = 0)
    {
        $partitionInfo = $this->zookeeper->getPartitionState($topicName, $partitionId);
        if (!$partitionInfo) {
            throw new \Kafka\Exception('topic:' . $topicName . ', partition id: ' . $partitionId . ' is not exists.');    
        }

        $hostList = $this->getBrokers();
        if (isset($partitionInfo['leader']) && isset($hostList[$partitionInfo['leader']])) {
            return $hostList[$partitionInfo['leader']]; 
        } else {
            throw new \Kafka\Exception('can\'t find broker host.');    
        }
    }

    // }}}
    // {{{ public function getZooKeeper()

    /**
     * get kafka zookeeper object 
     * 
     * @access public
     * @return \Kafka\ZooKeeper
     */
    public function getZooKeeper()
    {
        return $this->zookeeper; 
    }

    // }}}
    // {{{ public function getStream()

    /**
     * get broker broker connect 
     * 
     * @param string $host 
     * @access private
     * @return void
     */
    public function getStream($host)
    {
        if (!isset(self::$stream[$host])) {
            list($hostname, $port) = explode(':', $host);
            self::$stream[$host] = new \Kafka\Socket($hostname, $port);
            self::$stream[$host]->connect();
        }

        return self::$stream[$host];
    }

    // }}}
    // }}}
}
