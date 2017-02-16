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
class SimpleProduce
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
     * encoder
     *
     * @var mixed
     * @access private
     */
    private $encoder = null;
    /**
     * decoder
     *
     * @var mixed
     * @access private
     */
    private $decoder = null;
    /**
     * topic
     *
     * @var string
     * @access private
     */
    private $topic = '';
    /**
     * partition
     *
     * @var string
     * @access private
     */
    private $partition = '';
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
    // }}}
    // {{{ functions
    // {{{ public function static getInstance()
    /**
     * set send messages
     *
     * @access public
     * @param $hostList
     * @param $timeout
     * @param null $kafkaHostList
     * @return Produce
     */
    public static function getInstance($hostList, $timeout, $kafkaHostList = null)
    {
        if (is_null(self::$instance)) {
            self::$instance = new self($hostList, $timeout, $kafkaHostList);
        }
        return self::$instance;
    }
    // }}}
    // {{{ public function __construct()
    /**
     * __construct
     *
     * @access public
     * @param $hostList
     * @param null $timeout
     * @param null $kafkaHostList
     */
    public function __construct($hostList, $timeout = null, $kafkaHostList = null)
    {
        if ($hostList instanceof \Kafka\ClusterMetaData) {
            $metadata = $hostList;
        } elseif ($kafkaHostList !== null) {
            $metadata = new \Kafka\MetaDataFromKafka($kafkaHostList);
        } else {
            $metadata = new \Kafka\ZooKeeper($hostList, $timeout);
        }
        $this->client = new \Kafka\Client($metadata);
    }
    // }}}
    // {{{ public function setTopic()
    /**
     * set topic
     *
     * @access public
     * @param $topicName
     * @param int $partitionId
     * @return Produce
     */
    public function setTopic($topicName, $partitionId = 0)
    {
        $this->topic = $topicName;
        $this->partition = $partitionId;
        $host = $this->client->getHostByPartition($topicName, $partitionId);
        $stream = $this->client->getStream($host);
        $conn   = $stream['stream'];
        $this->encoder = new \Kafka\Protocol\Encoder($conn);
        if ((int) $this->requiredAck !== 0) { // get broker response
            $this->decoder = new \Kafka\Protocol\Decoder($conn);
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
     * @return Produce
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
     * @return Produce
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
     * @return bool|array $messages
     */
    public function send($messages)
    {
        if (empty($this->topic) || empty($messages)) {
            return;
        }
        $requestData = $this->_formatPayload($messages);
        $this->encoder->produceRequest($requestData);
        $response = array();
        if ((int) $this->requiredAck !== 0) { // get broker response
            $response = $this->decoder->produceResponse();
        }
        return $response;
    }
    // }}}
    // {{{ public function getClient()
    /**
     * get client object
     *
     * @access public
     * @return Client
     */
    public function getClient()
    {
        return $this->client;
    }
    /**
     * passthru method to client for setting stream options
     *
     * @access public
     * @param array $options
     */
    public function setStreamOptions($options = array())
    {
        $this->client->setStreamOptions($options);
    }
    // }}}
    // {{{ public function getAvailablePartitions()
    /**
     * get available partition
     *
     * @access public
     * @param $topicName
     * @return array
     */
    public function getAvailablePartitions()
    {
        if (empty($this->topic)) {
            return array();
        }
        $topicDetail = $this->client->getTopicDetail($this->topic);
        if (is_array($topicDetail) && isset($topicDetail['partitions'])) {
            $topicPartitiions = array_keys($topicDetail['partitions']);
        } else {
            $topicPartitiions = array();
        }
        return $topicPartitiions;
    }
    // }}}
    // {{{ private function _formatPayload()
    /**
     * format payload array
     *
     * @access private
     * @return array
     */
    private function _formatPayload($messages)
    {
        $requestData = array();
        $topicData = array();
        $partitionData[] = array(
            'partition_id' => $this->partition,
            'messages'     => $messages,
        );
        $topicData[] = array(
            'topic_name' => $this->topic,
            'partitions' => $partitionData,
        );
        $requestData = array(
            'required_ack' => $this->requiredAck,
            'timeout'      => $this->timeout,
            'data' => $topicData,
        );
        return $requestData;
    }
    // }}}
    // }}}
}
