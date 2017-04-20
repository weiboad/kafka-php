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

namespace Kafka\Protocol;

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

class Decoder extends Protocol
{
    // {{{ functions
    // {{{ public function produceResponse()

    /**
     * decode produce response
     *
     * @access public
     * @return array
     */
    public function produceResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('produce response invalid.');
        }
        $data = $this->stream->read($dataLen, true);

        // parse data struct
        $offset = 4;
        $version = $this->getApiVersion(self::PRODUCE_REQUEST);
        $ret = $this->decodeArray(substr($data, $offset), array($this, 'produceTopicPair'), $version);
        $offset += $ret['length'];
        $throttleTime = 0;
        if ($version == self::API_VERSION2) {
            $throttleTime = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        }
        return array('throttleTime' => $throttleTime, 'data' => $ret['data']);
    }

    // }}}
    // {{{ public function metadataResponse()

    /**
     * decode metadata response
     *
     * @access public
     * @return array
     */
    public function metadataResponse()
    {
        $broker = array();
        $topic = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('metaData response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;

        $version = $this->getApiVersion(self::METADATA_REQUEST);
        $brokerRet = $this->decodeArray(substr($data, $offset), array($this, 'metaBroker'), $version);
        $offset += $brokerRet['length'];
        $topicMetaRet = $this->decodeArray(substr($data, $offset), array($this, 'metaTopicMetaData'), $version);
        $offset += $topicMetaRet['length'];

        $result = array(
            'brokers' => $brokerRet['data'],
            'topics'  => $topicMetaRet['data'],
        );
        return $result;
    }

    // }}}
    // {{{ public function fetchResponse()

    /**
     * decode fetch response
     *
     * @access public
     * @return \Iterator
     */
    public function fetchResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('produce response invalid.');
        }
        $data = $this->stream->read($dataLen, true);

        // parse data struct
        $offset = 4;
        $version = $this->getApiVersion(self::FETCH_REQUEST);

        $throttleTime = 0;
        if ($version != self::API_VERSION0) {
            $throttleTime = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $offset += 4;
        }

        $topics = $this->decodeArray(substr($data, $offset), array($this, 'fetchTopic'), $version);
        $offset += $topics['length'];

        return array(
            'throttleTime' => $throttleTime,
            'topics' => $topics['data'],
        );
    }

    // }}}
    // {{{ public function offsetResponse()

    /**
     * decode offset response
     *
     * @access public
     * @return array
     */
    public function offsetResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('offset response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;

        $version = $this->getApiVersion(self::OFFSET_REQUEST);
        $topics = $this->decodeArray(substr($data, $offset), array($this, 'offsetTopic'), $version);
        $offset += $topics['length'];
        
        return $topics['data'];
    }

    // }}}
    // {{{ public function groupResponse()

    /**
     * decode group response
     *
     * @access public
     * @return array
     */
    public function groupResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('offset response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;

        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2; 
        $coordinatorId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4; 
        $hosts = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $hosts['length'];
        $coordinatorPort = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;

        return array(
            'errorCode' => $errorCode,
            'coordinatorId' => $coordinatorId,
            'coordinatorHost' => $hosts['data'],
            'coordinatorPort' => $coordinatorPort
        );
    }

    // }}}
    // {{{ public function joinGroupResponse()

    /**
     * decode join group response
     *
     * @access public
     * @return array
     */
    public function joinGroupResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('offset response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;

        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2; 
        $generationId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4; 
        $groupProtocol = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $groupProtocol['length'];
        $leaderId = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $leaderId['length'];
        $memberId = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $memberId['length'];

        $members = $this->decodeArray(substr($data, $offset), array($this, 'joinGroupMember'));
        $offset += $memberId['length'];

        return array(
            'errorCode' => $errorCode,
            'generationId' => $generationId,
            'groupProtocol' => $groupProtocol['data'],
            'leaderId' => $leaderId['data'],
            'memberId' => $memberId['data'],
            'members' => $members['data'],
        );
    }

    // }}}
    // {{{ public function syncGroupResponse()

    /**
     * decode sync group response
     *
     * @access public
     * @return array
     */
    public function syncGroupResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('offset response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;

        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2; 

        $memberAssignments = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset += $memberAssignments['length']; 

        $memberAssignment = $memberAssignments['data'];
        $memberAssignmentOffset = 0;
        $version = self::unpack(self::BIT_B16_SIGNED, substr($memberAssignment, $memberAssignmentOffset, 2));
        $memberAssignmentOffset += 2; 
        $partitionAssignments = $this->decodeArray(substr($memberAssignment, $memberAssignmentOffset),
                                array($this, 'syncGroupResponsePartition'));
        $memberAssignmentOffset += $partitionAssignments['length'];
        $userData = $this->decodeString(substr($memberAssignment, $memberAssignmentOffset), self::BIT_B32);
        
        return array(
            'errorCode' => $errorCode,
            'partitionAssignments' => $partitionAssignments['data'],
            'version' => $version,
            'userData' => $userData['data'],
        );
    }

    // }}}
    
    // {{{ public function commitOffsetResponse()

    /**
     * decode commit offset response
     *
     * @access public
     * @return array
     */
    public function commitOffsetResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        $dataLen = array_shift($dataLen);
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('commit offset response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;
        $topicCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $topicCount = array_shift($topicCount);
        for ($i = 0; $i < $topicCount; $i++) {
            $topicLen = self::unpack(self::BIT_B16, substr($data, $offset, 2)); // int16 topic name length
            $topicLen = isset($topicLen[1]) ? $topicLen[1] : 0;
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen);
            $offset += $topicLen;
            $partitionCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $offset += 4;
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $errCode     = self::unpack(self::BIT_B16, substr($data, $offset, 2));
                $offset += 2;
                $result[$topicName][$partitionId[1]] = array(
                    'errCode' => $errCode[1],
                );
            }
        }
        return $result;
    }

    // }}}
    // {{{ public function fetchOffsetResponse()

    /**
     * decode fetch offset response
     *
     * @access public
     * @return array
     */
    public function fetchOffsetResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        $dataLen = array_shift($dataLen);
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('fetch offset response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;
        $topicCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $topicCount = array_shift($topicCount);
        for ($i = 0; $i < $topicCount; $i++) {
            $topicLen = self::unpack(self::BIT_B16, substr($data, $offset, 2)); // int16 topic name length
            $topicLen = isset($topicLen[1]) ? $topicLen[1] : 0;
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen);
            $offset += $topicLen;
            $partitionCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $offset += 4;
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $partitionOffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
                $offset += 8;
                $metaLen = self::unpack(self::BIT_B16, substr($data, $offset, 2));
                $metaLen = array_shift($metaLen);
                $offset += 2;
                $metaData = '';
                if ($metaLen) {
                    $metaData = substr($data, $offset, $metaLen);
                    $offset += $metaLen;
                }
                $errCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
                $offset += 2;
                $result[$topicName][$partitionId[1]] = array(
                    'offset'   => $partitionOffset,
                    'metadata' => $metaData,
                    'errCode'  => $errCode[1],
                );
            }
        }
        return $result;
    }

    // }}}
    // {{{ public static function getError()

    /**
     * get error
     *
     * @param integer $errCode
     * @static
     * @access public
     * @return string
     */
    public static function getError($errCode)
    {
        switch ($errCode) {
            case 0:
                $error = 'No error--it worked!';
                break;
            case -1:
                $error = 'An unexpected server error';
                break;
            case 1:
                $error = 'The requested offset is outside the range of offsets maintained by the server for the given topic/partition.';
                break;
            case 2:
                $error = 'This indicates that a message contents does not match its CRC';
                break;
            case 3:
                $error = 'This request is for a topic or partition that does not exist on this broker.';
                break;
            case 4:
                $error = 'The message has a negative size';
                break;
            case 5:
                $error = 'This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes';
                break;
            case 6:
                $error = 'This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.';
                break;
            case 7:
                $error = 'This error is thrown if the request exceeds the user-specified time limit in the request.';
                break;
            case 8:
                $error = 'This is not a client facing error and is used only internally by intra-cluster broker communication.';
                break;
            case 9:
                $error = 'The replica is not available for the requested topic-partition';
                break;
            case 10:
                $error = 'The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.';
                break;
            case 11:
                $error = 'Internal error code for broker-to-broker communication.';
                break;
            case 12:
                $error = 'If you specify a string larger than configured maximum for offset metadata';
                break;
            case 13:
                $error = 'The server disconnected before a response was received.';
                break;
            case 14:
                $error = 'The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).';
                break;
            case 15:
                $error = 'The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.';
                break;
            case 16:
                $error = 'The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.';
                break;
            case 17:
                $error = 'The request attempted to perform an operation on an invalid topic.';
                break;
            case 18:
                $error = 'The request included message batch larger than the configured segment size on the server.';
                break;
            case 19:
                $error = 'Messages are rejected since there are fewer in-sync replicas than required.';
                break;
            case 20:
                $error = 'Messages are written to the log, but to fewer in-sync replicas than required.';
                break;
            case 21:
                $error = 'Produce request specified an invalid value for required acks.';
                break;
            case 22:
                $error = 'Specified group generation id is not valid.';
                break;
            case 23:
                $error = 'The group member\'s supported protocols are incompatible with those of existing members.';
                break;
            case 24:
                $error = 'The configured groupId is invalid';
                break;
            case 25:
                $error = 'The coordinator is not aware of this member.';
                break;
            case 26:
                $error = 'The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).';
                break;
            case 27:
                $error = 'The group is rebalancing, so a rejoin is needed.';
                break;
            case 28:
                $error = 'The committing offset data size is not valid';
                break;
            case 29:
                $error = 'Topic authorization failed.';
                break;
            case 30:
                $error = 'Group authorization failed.';
                break;
            case 31:
                $error = 'Cluster authorization failed.';
                break;
            case 32:
                $error = 'The timestamp of the message is out of acceptable range.';
                break;
            case 33:
                $error = 'The broker does not support the requested SASL mechanism.';
                break;
            case 34:
                $error = 'Request is not valid given the current SASL state.';
                break;
            case 35:
                $error = 'The version of API is not supported.';
                break;
            default:
                $error = 'Unknown error';
        }

        return $error;
    }

    // }}}
    
    // {{{ protected function produceTopicPair()

    /**
     * decode produce topic pair response
     *
     * @access protected
     * @return array
     */
    protected function produceTopicPair($data, $version)
    {
        $offset = 0;
        $topicInfo = $this->decodeString($data, self::BIT_B16);
        $offset += $topicInfo['length'];
        $ret = $this->decodeArray(substr($data, $offset), array($this, 'producePartitionPair'), $version);
        $offset += $ret['length'];

        return array('length' => $offset, 'data' => array(
            'topicName' => $topicInfo['data'],
            'partitions'=> $ret['data'],
        ));
    }

    // }}}
    // {{{ protected function producePartitionPair()

    /**
     * decode produce partition pair response
     *
     * @access protected
     * @return array
     */
    protected function producePartitionPair($data, $version)
    {
        $offset = 0;
        $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $partitionOffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
        $offset += 8;
        $timestamp = 0;
        if ($version == self::API_VERSION2) {
            $timestamp = self::unpack(self::BIT_B64, substr($data, $offset, 8));
            $offset += 8;
        }

        return array(
            'length' => $offset, 
            'data'   => array(
                'partition' => $partitionId,
                'errorCode' => $errorCode,
                'offset' => $offset,
                'timestamp' => $timestamp,
            )
        );
    }

    // }}}
    
    // {{{ protected function metaBroker()

    /**
     * decode meta broker response
     *
     * @access protected
     * @return array
     */
    protected function metaBroker($data, $version)
    {
        $offset = 0;
        $nodeId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $hostNameInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $hostNameInfo['length'];
        $port = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        return array(
            'length' => $offset, 
            'data' => array(
                'host' => $hostNameInfo['data'], 
                'port' => $port, 
                'nodeId' => $nodeId
            )
        );
    }

    // }}}
    // {{{ protected function metaTopicMetaData()

    /**
     * decode meta topic meta data response
     *
     * @access protected
     * @return array
     */
    protected function metaTopicMetaData($data, $version)
    {
        $offset = 0;
        $topicErrCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $topicInfo['length'];
        $partionsMetaRet = $this->decodeArray(substr($data, $offset), array($this, 'metaPartitionMetaData'), $version);
        $offset += $partionsMetaRet['length'];

        return array(
            'length' => $offset, 
            'data' => array(
                'topicName' => $topicInfo['data'], 
                'errorCode' => $topicErrCode, 
                'partitions' => $partionsMetaRet['data'],
            )
        );
    }

    // }}}
    // {{{ protected function metaPartitionMetaData()

    /**
     * decode meta partition meta data response
     *
     * @access protected
     * @return array
     */
    protected function metaPartitionMetaData($data, $version)
    {
        $offset = 0;
        $errcode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $partId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $leader = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $replicas = $this->decodePrimitiveArray(substr($data, $offset), self::BIT_B32);
        $offset += $replicas['length'];
        $isr = $this->decodePrimitiveArray(substr($data, $offset), self::BIT_B32);
        $offset += $isr['length'];

        return array(
            'length' => $offset, 
            'data' => array(
                'partitionId' => $partId, 
                'errorCode' => $errcode, 
                'replicas' => $replicas['data'],
                'leader' => $leader,
                'isr' => $isr['data'],
            )
        );
    }

    // }}}

    // {{{ protected function fetchTopic()

    /**
     * decode fetch topic response
     *
     * @access protected
     * @return array
     */
    protected function fetchTopic($data, $version)
    {
        $offset = 0;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $topicInfo['length'];

        $partitions = $this->decodeArray(substr($data, $offset), array($this, 'fetchPartition'), $version);
        $offset += $partitions['length'];

        return array(
            'length' => $offset, 
            'data' => array(
                'topicName' => $topicInfo['data'], 
                'partitions'  => $partitions['data'], 
            )
        );
    }

    // }}}
    // {{{ protected function fetchPartition()

    /**
     * decode fetch partition response
     *
     * @access protected
     * @return array
     */
    protected function fetchPartition($data, $version)
    {
        $offset = 0;
        $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $highwaterMarkOffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
        $offset += 8;

        $messageSetSize = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;

        if ($offset < strlen($data)) {
            $messages = $this->decodeMessageSetArray(substr($data, $offset), array($this, 'decodeMessageSet'), $messageSetSize);
            $offset += $messages['length'];
        }

        return array(
            'length' => $offset, 
            'data' => array(
                'partition' => $partitionId, 
                'errorCode' => $errorCode, 
                'highwaterMarkOffset' => $highwaterMarkOffset, 
                'messageSetSize' => $messageSetSize,
                'messages' => isset($messages['data']) ? $messages['data'] : array(),
            )
        );
    }

    // }}}
    
    // {{{ protected function offsetTopic()

    /**
     * decode offset topic response
     *
     * @access protected
     * @return array
     */
    protected function offsetTopic($data, $version)
    {
        $offset = 0;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $topicInfo['length'];

        $partitions = $this->decodeArray(substr($data, $offset), array($this, 'offsetPartition'), $version);
        $offset += $partitions['length'];

        return array(
            'length' => $offset, 
            'data' => array(
                'topicName' => $topicInfo['data'], 
                'partitions'  => $partitions['data'], 
            )
        );
    }

    // }}}
    // {{{ protected function offsetPartition()

    /**
     * decode offset partition response
     *
     * @access protected
     * @return array
     */
    protected function offsetPartition($data, $version)
    {
        $offset = 0;
        $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $timestamp = 0;
        if ($version != self::API_VERSION0) {
            $timestamp = self::unpack(self::BIT_B64, substr($data, $offset, 8));
            $offset += 8;
        }
        $offsets = $this->decodePrimitiveArray(substr($data, $offset), self::BIT_B64);
        $offset += $offsets['length'];

        return array(
            'length' => $offset, 
            'data' => array(
                'partition' => $partitionId, 
                'errorCode' => $errorCode, 
                'timestamp' => $timestamp, 
                'offsets' => $offsets['data'],
            )
        );
    }

    // }}}
    
    // {{{ protected function joinGroupMember()

    /**
     * decode join group member response
     *
     * @access protected
     * @return array
     */
    protected function joinGroupMember($data)
    {
        $offset = 0;
        $memberId = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $memberId['length'];
        $memberMeta = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset += $memberMeta['length'];

        $metaData = $memberMeta['data'];
        $metaOffset = 0;
        $version = self::unpack(self::BIT_B16, substr($metaData, $metaOffset, 2));
        $metaOffset += 2;
        $topics = $this->decodeArray(substr($metaData, $metaOffset), array($this, 'decodeString'), self::BIT_B16);
        $metaOffset += $topics['length'];
        $userData = $this->decodeString(substr($metaData, $metaOffset), self::BIT_B32);

        return array(
            'length' => $offset, 
            'data' => array(
                'memberId' => $memberId['data'], 
                'memberMeta' => array(
                    'version' => $version,
                    'topics'  => $topics['data'],
                    'userData' => $userData['data'],
                ),
            )
        );
    }

    // }}}
    
    // {{{ protected function syncGroupResponsePartition()

    /**
     * decode sync group partition response
     *
     * @access protected
     * @return array
     */
    protected function syncGroupResponsePartition($data)
    {
        $offset = 0;
        $topicName = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $topicName['length'];
        $partitions = $this->decodePrimitiveArray(substr($data, $offset), self::BIT_B32);
        $offset += $partions['length'];

        return array(
            'length' => $offset, 
            'data' => array(
                'topicName' => $topicName['data'], 
                'partitions' => $partitions['data'],
            )
        );
    }

    // }}}
    // {{{ public function decodeString()

    /**
     * decode unpack string type
     *
     * @param bytes $data
     * @param int $bytes self::BIT_B32: int32 big endian order. self::BIT_B16: int16 big endian order.
     * @param int $compression
     * @return string
     * @access public
     */
    public function decodeString($data, $bytes, $compression = self::COMPRESSION_NONE)
    {
        $offset = ($bytes == self::BIT_B32) ? 4 : 2;
        $packLen = self::unpack($bytes, substr($data, 0, $offset)); // int16 topic name length
        $data = substr($data, $offset, $packLen);
        $offset += $packLen;

        switch ($compression) {
            case self::COMPRESSION_NONE:
                break;
            case self::COMPRESSION_GZIP:
                $data = \gzdecode($data);
                break;
            case self::COMPRESSION_SNAPPY:
                // todo
                throw new \Kafka\Exception\NotSupported('SNAPPY compression not yet implemented');
            default:
                throw new \Kafka\Exception\NotSupported('Unknown compression flag: ' . $compression);
        }
        return array('length' => $offset, 'data' => $data);
    }

    // }}}
    // {{{ public function decodeArray()

    /**
     * decode key array
     *
     * @param array $array
     * @param Callable $func
     * @param null $options
     * @return string
     * @access public
     */
    public function decodeArray($data, $func, $options = null)
    {
        $offset = 0;
        $arrayCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;

        if (!is_callable($func, false)) {
            throw new \Kafka\Exception\Protocol('Decode array failed, given function is not callable.');
        }

        $result = array();
        for ($i = 0; $i < $arrayCount; $i++) {
            $value = substr($data, $offset);
            if (!is_null($options)) {
                $ret = call_user_func($func, $value, $options);
            } else {
                $ret = call_user_func($func, $value);
            }

            if (!is_array($ret) && $ret === false) {
                break;
            }

            if (!isset($ret['length']) || !isset($ret['data'])) {
                throw new \Kafka\Exception\Protocol('Decode array failed, given function return format is invliad');
            }
            if ($ret['length'] == 0) {
                continue;
            }

            $offset += $ret['length'];
            $result[] = $ret['data'];
        }

        return array('length' => $offset, 'data' => $result);
    }

    // }}}
    // {{{ public function decodePrimitiveArray()

    /**
     * decode primitive type array
     *
     * @param bytes[] $data
     * @param bites $bites
     * @return array
     * @access public
     */
    public function decodePrimitiveArray($data, $bites)
    {
        $offset = 0;
        $arrayCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;

        $result = array();
        for ($i = 0; $i < $arrayCount; $i++) {
            if ($bites == self::BIT_B64) {
                $result[] = self::unpack(self::BIT_B64, substr($data, $offset, 8));
                $offset += 8;
            } else if ($bites == self::BIT_B32) {
                $result[] = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
            } else if (in_array($bites, array(self::BIT_B16, self::BIT_B16_SIGNED))) {
                $result[] = self::unpack($bites, substr($data, $offset, 2));
                $offset += 2;
            } else if ($bites == self::BIT_B8) {
                $result[] = self::unpack($bites, substr($data, $offset, 1));
                $offset += 1;
            }
        }

        return array('length' => $offset, 'data' => $result);
    }

    // }}}
    
    // {{{ public function decodeMessageSetArray()

    /**
     * decode message Set
     *
     * @param array $array
     * @param Callable $func
     * @param null $options
     * @return string
     * @access public
     */
    public function decodeMessageSetArray($data, $func, $options = null)
    {
        $offset = 0;
        if (!is_callable($func, false)) {
            throw new \Kafka\Exception\Protocol('Decode array failed, given function is not callable.');
        }

        $result = array();
        while ($offset < strlen($data)) {
            $value = substr($data, $offset);
            if (!is_null($options)) {
                $ret = call_user_func($func, $value, $options);
            } else {
                $ret = call_user_func($func, $value);
            }

            if (!is_array($ret) && $ret === false) {
                break;
            }

            if (!isset($ret['length']) || !isset($ret['data'])) {
                throw new \Kafka\Exception\Protocol('Decode array failed, given function return format is invliad');
            }
            if ($ret['length'] == 0) {
                continue;
            }

            $offset += $ret['length'];
            $result[] = $ret['data'];
        
        }

        return array('length' => $offset, 'data' => $result);
    }

    // }}}
    // {{{ public function decodeMessageSet()

    /**
     * decode message set
     * N.B., MessageSets are not preceded by an int32 like other array elements
     * in the protocol.
     *
     * @param array $messages
     * @param int $compression
     * @return string
     * @access public
     */
    protected function decodeMessageSet($data, $length)
    {
        if (strlen($data) <= 12) {
            return false;
        }
        $offset = 0;
        $roffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
        $offset += 8;
        $messageSize = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $ret = $this->decodeMessage(substr($data, $offset), $messageSize);
        if (!is_array($ret) && $ret == false) {
            return false;
        }
        $offset += $ret['length'];

        return array(
            'length' => $offset,
            'data' => array(
                'offset' => $roffset,
                'size'   => $messageSize,
                'message' => $ret['data'],
            )
        );
    }

    // }}}
    // {{{ public function decodeMessage()

    /**
     * decode message 
     * N.B., MessageSets are not preceded by an int32 like other array elements
     * in the protocol.
     *
     * @param array $messages
     * @param int $compression
     * @return string
     * @access public
     */
    protected function decodeMessage($data, $messageSize)
    {
        if (strlen($data) < $messageSize) {
            return false;
        }

        $offset = 0;
        $crc = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $magic = self::unpack(self::BIT_B8, substr($data, $offset, 1));
        $offset += 1;
        $attr  = self::unpack(self::BIT_B8, substr($data, $offset, 1));
        $offset += 1;
        $timestamp = 0;
        $version = $this->getApiVersion(self::FETCH_REQUEST);
        if ($version == self::API_VERSION2) {
            $timestamp = self::unpack(self::BIT_B64, substr($data, $offset, 8));
            $offset += 8;
        }
        $keyRet = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset += $keyRet['length'];
        $valueRet = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset += $valueRet['length'];

        return array(
            'length' => $offset,
            'data'   => array(
                'crc' => $crc,
                'magic' => $magic,
                'attr' => $attr,
                'timestamp' => $timestamp,
                'key' => $keyRet['data'],
                'value' => $valueRet['data'],
            )
        );
    }

    // }}}
    // }}}
}
