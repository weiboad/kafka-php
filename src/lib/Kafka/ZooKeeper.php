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
        $path = sprintf(self::BROKER_DETAIL_PATH, (int) $brokerId);
        $result = $this->zookeeper->get($path);
        if (!$result) {
            return false;   
        }

        $result = json_decode($result, true);
        return $result;
    }

    // }}}
}
