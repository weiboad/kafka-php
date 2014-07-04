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

class ZooKeeper
{
    // {{{ consts

    /**
     * get all broker 
     */
    const BROKER_PATH = '/brokers/ids';

    /**
     * get broker detail 
     */
    const BROKER_DETAIL_PATH = '/brokers/ids/%d';

    /**
     * get topic detail  
     */
    const TOPIC_PATCH = '/brokers/topics/%s';

    /**
     * get partition state 
     */
    const PARTITION_STATE = '/brokers/topics/%s/partitions/%d/state';

    // }}}
    // {{{ members

    /**
     * zookeeper 
     * 
     * @var mixed
     * @access private
     */
    private $zookeeper = null;

    // }}} 
    // {{{ functions
    // {{{ public function __construct()

    /**
     * __construct 
     * 
     * @access public
     * @return void
     */
    public function __construct($host, $port)
    {
        $address = $host . ':' . $port;
        $this->zookeeper = new \Zookeeper($address); 
    }

    // }}}
    // {{{ public function listBrokers()
    
    /**
     * get broker list using zookeeper 
     * 
     * @access public
     * @return array
     */
    public function listBrokers()
    {
        $result = array();
        $lists = $this->zookeeper->getChildren(self::BROKER_PATH); 
        if (!empty($lists)) {
            foreach ($lists as $brokerId) {
                $brokerDetail = $this->getBrokerDetail($brokerId);    
                if (!$brokerDetail) {
                    continue;    
                }
                $result[$brokerId] = $brokerDetail;
            }    
        }

        return $result;
    }

    // }}}
    // {{{ public function getBrokerDetail()
    
    /**
     * get broker detail 
     * 
     * @param integer $brokerId 
     * @access public
     * @return void
     */
    public function getBrokerDetail($brokerId)
    { 
        $result = array();
        $path = sprintf(self::BROKER_DETAIL_PATH, (int) $brokerId);
        if ($this->zookeeper->exists($path)) {
            $result = $this->zookeeper->get($path);
            if (!$result) {
                return false;   
            }

            $result = json_decode($result, true);
        }

        return $result;
    }

    // }}}
    // {{{ public function getTopicDetail()
    
    /**
     * get topic detail 
     * 
     * @param string $topicName 
     * @access public
     * @return void
     */
    public function getTopicDetail($topicName)
    { 
        $result = array();
        $path = sprintf(self::TOPIC_PATCH, (string) $topicName);
        if ($this->zookeeper->exists($path)) {
            $result = $this->zookeeper->get($path);
            if (!$result) {
                return false;   
            }
            $result = json_decode($result, true);
        }

        return $result;
    }

    // }}}
    // {{{ public function getPartitionState()
    
    /**
     * get partition state 
     * 
     * @param string $topicName 
     * @param integer $partitionId 
     * @access public
     * @return void
     */
    public function getPartitionState($topicName, $partitionId = 0)
    { 
        $result = array();
        $path = sprintf(self::PARTITION_STATE, (string) $topicName, (int) $partitionId);
        if ($this->zookeeper->exists($path)) {
            $result = $this->zookeeper->get($path);
            if (!$result) {
                return false;   
            }
            $result = json_decode($result, true);
        }

        return $result;
    }

    // }}}
    // }}}
}
