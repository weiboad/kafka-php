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
        $dataLen = array_shift($dataLen);
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('produce response invalid.');
        }
        $data = $this->stream->read($dataLen, true);

        // parse data struct
        $offset = 4;
        $topicCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $topicCount = array_shift($topicCount);
        $offset += 4;
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
                $errCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
                $offset += 2;
                $partitionOffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
                $offset += 8;
                $result[$topicName][$partitionId[1]] = array(
                    'errCode' => $errCode[1],
                    'offset'  => $partitionOffset
                );
            }
        }

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
        return new \Kafka\Protocol\Fetch\Topic($this->stream);
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
        $dataLen = array_shift($dataLen);
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('metaData response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;
        $brokerCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $brokerCount = isset($brokerCount[1]) ? $brokerCount[1] : 0;
        for ($i = 0; $i < $brokerCount; $i++) {
            $nodeId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $nodeId = $nodeId[1];
            $offset += 4;
            $hostNameLen = self::unpack(self::BIT_B16, substr($data, $offset, 2)); // int16 host name length
            $hostNameLen = isset($hostNameLen[1]) ? $hostNameLen[1] : 0;
            $offset += 2;
            $hostName = substr($data, $offset, $hostNameLen);
            $offset += $hostNameLen;
            $port = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $offset += 4;
            $broker[$nodeId] = array(
                'host' => $hostName,
                'port' => $port[1],
            );
        }

        $topicMetaCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $topicMetaCount = isset($topicMetaCount[1]) ? $topicMetaCount[1] : 0;
        for ($i = 0; $i < $topicMetaCount; $i++) {
            $topicErrCode = self::unpack(self::BIT_B16, substr($data, $offset, 2));
            $offset += 2;
            $topicLen = self::unpack(self::BIT_B16, substr($data, $offset, 2));
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen[1]);
            $offset += $topicLen[1];
            $partitionCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $offset += 4;
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $topic[$topicName]['errCode'] = $topicErrCode[1];
            $partitions = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionErrCode = self::unpack(self::BIT_B16, substr($data, $offset, 2));
                $offset += 2;
                $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $partitionId = isset($partitionId[1]) ? $partitionId[1] : 0;
                $offset += 4;
                $leaderId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $repliasCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $repliasCount = isset($repliasCount[1]) ? $repliasCount[1] : 0;
                $replias = array();
                for ($z = 0; $z < $repliasCount; $z++) {
                    $repliaId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                    $offset += 4;
                    $replias[] = $repliaId[1];
                }
                $isrCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $isrCount = isset($isrCount[1]) ? $isrCount[1] : 0;
                $isrs = array();
                for ($z = 0; $z < $isrCount; $z++) {
                    $isrId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                    $offset += 4;
                    $isrs[] = $isrId[1];
                }

                $partitions[$partitionId] = array(
                    'errCode'  => $partitionErrCode[1],
                    'leader'   => $leaderId[1],
                    'replicas' => $replias,
                    'isr'      => $isrs,
                );
            }
            $topic[$topicName]['partitions'] = $partitions;
        }

        $result = array(
            'brokers' => $broker,
            'topics'  => $topic,
        );
        return $result;
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
        $dataLen = array_shift($dataLen);
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('offset response invalid.');
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
                $offsetCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $offsetCount = array_shift($offsetCount);
                $offsetArr = array();
                for ($z = 0; $z < $offsetCount; $z++) {
                    $offsetArr[] = self::unpack(self::BIT_B64, substr($data, $offset, 8));
                    $offset += 8;
                }
                $result[$topicName][$partitionId[1]] = array(
                    'errCode' => $errCode[1],
                    'offset'  => $offsetArr
                );
            }
        }
        return $result;
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
    // }}}
}
