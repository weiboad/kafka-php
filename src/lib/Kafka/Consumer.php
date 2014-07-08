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

class Consumer
{
    // {{{ consts
    // }}}
    // {{{ members

    /**
     * client 
     * 
     * @var mixed
     * @access private
     */
    private $client = null;

    /**
     * send message options cache 
     * 
     * @var array
     * @access private
     */
    private $payload = array();

    /**
     * consumer group 
     * 
     * @var string
     * @access private
     */
    private $group = '';

    /**
     * from offset 
     * 
     * @var mixed
     * @access private
     */
    private $fromOffset = true;

    /**
     * produce instance 
     * 
     * @var \Kafka\Produce
     * @access private
     */
    private static $instance = null;

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
    private $stream = array();

    // }}} 
    // {{{ functions
    // {{{ public function static getInstance()

    /**
     * set send messages  
     * 
     * @access public
     * @return void
     */
    public static function getInstance($host, $port)
    {
        if (is_null(self::$instance)) {
            self::$instance = new self($host, $port); 
        }
       
        return self::$instance; 
    }

    // }}}
    // {{{ private function __construct()

    /**
     * __construct 
     * 
     * @access public
     * @return void
     */
    private function __construct($host, $port)
    {
        $zookeeper = new \Kafka\ZooKeeper($host, $port);
        $this->client = new \Kafka\Client($zookeeper);
    }

    // }}}
    // {{{ public function setPartition()

    /**
     * set topic partition  
     * 
     * @access public
     * @return void
     */
    public function setPartition($topicName, $partitionId = 0, $offset = null)
    {
        if (is_null($offset)) {
            if ($this->fromOffset) {
                $offsetObject = new \Kafka\Offset($this->client, $this->group, $topicName, $partitionId);  
                $offset = $offsetObject->getOffset(0);
            } else {
                $offset = 0;    
            }    
        }
        $this->payload[$topicName][$partitionId] = $offset;    
        
        return $this; 
    }

    // }}}
    // {{{ public function setFromOffset()

    /**
     * set whether starting offset fetch 
     * 
     * @param boolean $fromOffset 
     * @access public
     * @return void
     */
    public function setFromOffset($fromOffset)
    {
        $this->fromOffset = (boolean) $fromOffset; 
    }

    // }}}
    // {{{ public function setGroup()
    
    /**
     * set consumer group
     * 
     * @param string $group 
     * @access public
     * @return void
     */
    public function setGroup($group)
    {
        $this->group = (string) $group;
        return $this;
    }

    // }}}
    // {{{ public function fetch()
    
    /**
     * fetch message to broker 
     * 
     * @access public
     * @return void
     */
    public function fetch()
    {
        $data = $this->_formatPayload();
        if (empty($data)) {
            return false;    
        }

        $responseData = array();
        $streams = array();
        foreach ($data as $host => $requestData) {
            $connArr = $this->client->getStream($host);
            $conn    = $connArr['stream'];
            $encoder = new \Kafka\Protocol\Encoder($conn);     
            $encoder->fetchRequest($requestData);
            $streams[$connArr['key']] = $conn;
        }
            
        $fetch = new \Kafka\Protocol\Fetch\Topic($streams);

        // register fetch helper
        $freeStream = new \Kafka\Protocol\Fetch\Helper\FreeStream($this->client);
        $freeStream->setStreams($streams);
        \Kafka\Protocol\Fetch\Helper\Helper::registerHelper('freeStream', $freeStream);

        // register partition commit offset
        $commitOffset = new \Kafka\Protocol\Fetch\Helper\CommitOffset($this->client);
        $commitOffset->setGroup($this->group);
        \Kafka\Protocol\Fetch\Helper\Helper::registerHelper('commitOffset', $commitOffset);

        return $fetch;
    }

    // }}}
    // {{{ public function getClient()
    
    /**
     * get client object 
     * 
     * @access public
     * @return void
     */
    public function getClient()
    {
        return $this->client;
    }

    // }}}
    // {{{ private function _formatPayload()
    
    /**
     * format payload array 
     * 
     * @access private
     * @return array
     */
    private function _formatPayload()
    {
        if (empty($this->payload)) {
            return array();    
        }

        $data = array();
        foreach ($this->payload as $topicName => $partitions) {
            foreach ($partitions as $partitionId => $offset) {
                $host = $this->client->getHostByPartition($topicName, $partitionId); 
                $data[$host][$topicName][$partitionId] = $offset;
            }     
        }
        
        $requestData = array();
        foreach ($data as $host => $info) {
            $topicData = array();
            foreach ($info as $topicName => $partitions) {
                $partitionData = array();
                foreach ($partitions as $partitionId => $offset) {
                    $partitionData[] = array(
                        'partition_id' => $partitionId,
                        'offset'       => $offset,
                    );
                }
                $topicData[] = array(
                    'topic_name' => $topicName,
                    'partitions' => $partitionData,
                );
            }    

            $requestData[$host] = array(
                'data' => $topicData,
            );
        }
       
       return $requestData; 
    }

    // }}}
    // }}}
}
