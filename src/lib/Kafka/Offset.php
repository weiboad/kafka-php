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

class Offset
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
     * consumer group 
     * 
     * @var string
     * @access private
     */
    private $groupId = '';

    /**
     * topic name 
     * 
     * @var string
     * @access private
     */
    private $topicName = '';

    /**
     * topic partition id, default 0 
     * 
     * @var float
     * @access private
     */
    private $partitionId = 0;

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

    // }}} 
    // {{{ functions
    // {{{ public function __construct()

    /**
     * __construct 
     * 
     * @access public
     * @return void
     */
    public function __construct($client, $groupId, $topicName, $partitionId = 0)
    {
        $this->client      = $client;
        $this->groupId     = $groupId;
        $this->topicName   = $topicName;
        $this->partitionId = $partitionId;

        $host   = $this->client->getHostByPartition($topicName, $partitionId);
        $stream = $this->client->getStream($host);
        $this->encoder = new \Kafka\Protocol\Encoder($stream);
        $this->decoder = new \Kafka\Protocol\Decoder($stream);
    }

    // }}}
    // {{{ public function setOffset()
    
    /**
     * set consumer offset 
     * 
     * @param integer $offset 
     * @access public
     * @return void
     */
    public function setOffset($offset)
    {
        $maxOffset = $this->getProduceOffset();
        if ($offset > $maxOffset) {
            throw new \Kafka\Exception('this offset is invalid. must less than max offset:' . $maxOffset);
        }

        $data = array(
            'group_id' => $this->groupId,
            'data' => array(
                array(
                    'topic_name' => $this->groupId,
                    'partitions' => array(
                        array(
                            'partition_id' => $this->partitionId,
                            'offset' => $offset,
                            ),
                        ),
                ),
            ),
        );
   
        $this->encoder->commitOffsetRequest($requestData);
        $result = $this->decoder->commitOffsetResponse();
        if (!isset($result[$topicName][$partitionId]['errCode'])) {
            throw new \Kafka\Exception('commit topic offset failed.');
        }
        if ($result[$topicName][$partitionId]['errCode'] != 0) {
            throw new \Kafka\Exception(\Kafka\Protocol\Decoder::getError($result[$topicName][$partitionId]['errCode']));
        }
    }
     
    // }}}
    // {{{ public function getOffset()
    
    /**
     * get consumer offset 
     * 
     * @param integer $defaultOffset 
     * @access public
     * @return void
     */
    public function getOffset($defaultOffset = null)
    {
        $data = array(
            'group_id' => $this->groupId,
            'data' => array(
                array(
                    'topic_name' => $this->groupId,
                    'partitions' => array(
                        array(
                            'partition_id' => $this->partitionId,
                        ),
                    ),
                ),
            ),
        );
   
        $this->encoder->fetchOffsetRequest($requestData);
        $result = $this->decoder->fetchOffsetResponse();
        if (!isset($result[$topicName][$partitionId]['errCode'])) {
            throw new \Kafka\Exception('fetch topic offset failed.');
        }

        if ($result[$topicName][$partitionId]['errCode'] == 3) {
            if ($defaultOffset) {
                $this->setOffset($defaultOffset);
                return $defaultOffset;
            }
        } else if ($result[$topicName][$partitionId]['errCode'] == 0) {
            return $result[$topicName][$partitionId]['offset'];
        } else {
            throw new \Kafka\Exception(\Kafka\Protocol\Decoder::getError($result[$topicName][$partitionId]['errCode']));
        }
    }
     
    // }}}
    // {{{ public function getProduceOffset()
    
    /**
     * get produce server offset 
     * 
     * @param string $topicName 
     * @param integer $partitionId 
     * @access public
     * @return int
     */
    public function getProduceOffset()
    {
        $requestData = array(
            'data' => array(
                array(
                    'topic_name' => $this->topicName,
                    'partitions' => array(
                        array(
                            'partition_id' => $this->partitionId,
                            'time' => -1,
                            'max_offset' => 1,
                        ),
                    ),
                ),
            ),
        );
        $this->encoder->offsetRequest($requestData);
        $result = $this->decoder->offsetResponse();
        
        if (!isset($result[$topicName][$partitionId]['offset'])) {
            if (isset($result[$topicName][$partitionId]['errCode'])) {
                throw new \Kafka\Exception(\Kafka\Protocol\Decoder::getError($result[$topicName][$partitionId]['errCode']));    
            } else {
                throw new \Kafka\Exception('get offset failed. topic name:' . $topicName . ' partitionId: ' . $partitionId);    
            }
        }

        return array_shift($result[$topicName][$partitionId]['offset']);
    }

    // }}}
    // }}}
}
