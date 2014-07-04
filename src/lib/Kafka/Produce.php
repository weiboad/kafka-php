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

class Produce
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
     * send message options cache 
     * 
     * @var array
     * @access private
     */
    private $payload = array();

    /**
     * default the server will not send any response 
     * 
     * @var float
     * @access private
     */
    private $requiredAck = 0;

    /**
     * default timeout is 100ms 
     * 
     * @var float
     * @access private
     */
    private $timeout = 100;

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
        $this->zookeeper = new \Kafka\ZooKeeper('localhost', 2181);
    }

    // }}}
    // {{{ public function setMessages()

    /**
     * set send messages  
     * 
     * @access public
     * @return void
     */
    public function setMessages($topicName, $partitionId = 0, $messages = array())
    {
        if (isset($this->payload[$topicName][$partitionId])) {
            $this->payload[$topicName][$partitionId] = 
                    array_merge($messages, $this->payload[$topicName][$partitionId]);    
        } else {
            $this->payload[$topicName][$partitionId] = $messages;    
        }
        
        return $this; 
    }

    // }}}
    // {{{ public function setRequireAck()
    
    /**
     * set request mode
     * This field indicates how many acknowledgements the servers should receive
     * before responding to the request. If it is 0 the server will not send any
     * response (this is the only case where the server will not reply to a
     * request). If it is 1, the server will wait the data is written to the
     * local log before sending a response. If it is -1 the server will block
     * until the message is committed by all in sync replicas before sending a
     * response. For any number > 1 the server will block waiting for this
     * number of acknowledgements to occur (but the server will never wait for
     * more acknowledgements than there are in-sync replicas). 
     * 
     * @param int $ack 
     * @access public
     * @return void
     */
    public function setRequireAck($ack = 0)
    {
        if ($ack >= -1) {
            $this->requiredAck = (int) $ack;    
        }

        return $this;
    }

    // }}}
    // {{{ public function setTimeOut()
    
    /**
     * set request timeout
     * 
     * @param int $timeout 
     * @access public
     * @return void
     */
    public function setTimeOut($timeout = 100)
    {
        if ((int) $timeout) {
            $this->timeout = (int) $timeout;   
        }
        return $this;
    }

    // }}}
    // {{{ public function send()
    
    /**
     * send message to broker 
     * 
     * @access public
     * @return void
     */
    public function send()
    {
        $data = $this->_formatPayload();
        if (empty($data)) {
            return false;    
        }

        $responseData = array();
        foreach ($data as $host => $requestData) {
            $conn = $this->_getStream($host);
            $requestData = \Kafka\Protocol\Encoder::buildProduceRequest($requestData);     
            $conn->write($requestData);
            if ((int) $this->requiredAck !== 0) { // get broker response
                $dataLen = unpack('N', $conn->read(4));
                $dataLen = array_shift($dataLen);
                if (!$dataLen) {
                    throw new \Kafka\Exception('send message failed. response null.');
                }
                $response = $conn->read($dataLen); 
                $response = \Kafka\Protocol\Decoder::decodeProduceResponse($response);
                foreach ($response as $topicName => $info) {
                    if (!isset($responseData[$topicName])) {
                        $responseData[$topicName] = $info; 
                    } else {
                        $responseData[$topicName] = array_merge($info, $responseData[$topicName]);    
                    }
                }
            }
        }

        return $responseData;
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
            foreach ($partitions as $partitionId => $messages) {
                $host = $this->getHostByPartition($topicName, $partitionId); 
                $data[$host][$topicName][$partitionId] = $messages;
            }     
        }
        
        $requestData = array();
        foreach ($data as $host => $info) {
            $topicData = array();
            foreach ($info as $topicName => $partitions) {
                $partitionData = array();
                foreach ($partitions as $partitionId => $messages) {
                    $partitionData[] = array(
                        'partition_id' => $partitionId,
                        'messages'     => $messages,
                    );
                }
                $topicData[] = array(
                    'topic_name' => $topicName,
                    'partitions' => $partitionData,
                );
            }    

            $requestData[$host] = array(
                'required_ack' => $this->requiredAck,
                'timeout'      => $this->timeout,
                'data' => $topicData,
            );
        }
       
       return $requestData; 
    }

    // }}}
    // {{{ private function _getStream()

    /**
     * get broker broker connect 
     * 
     * @param string $host 
     * @access private
     * @return void
     */
    private function _getStream($host)
    {
        if (!isset($this->stream[$host])) {
            list($hostname, $port) = explode(':', $host);
            $this->stream[$host] = new \Kafka\Socket($hostname, $port);
            $this->stream[$host]->connect();
        }

        return $this->stream[$host];
    }

    // }}}
    // }}}
}
