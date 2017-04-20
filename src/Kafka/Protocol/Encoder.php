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

class Encoder extends Protocol
{
    // {{{ functions
    // {{{ public function produceRequest()

    /**
     * produce request
     *
     * @param array $payloads
     * @param int $compression
     * @return int
     * @access public
     */
    public function produceRequest($payloads, $compression = self::COMPRESSION_NONE)
    {
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception\Protocol('given procude data invalid. `data` is undefined.');
        }

        if (!isset($payloads['required_ack'])) {
            // default server will not send any response
            // (this is the only case where the server will not reply to a request)
            $payloads['required_ack'] = 0;
        }

        if (!isset($payloads['timeout'])) {
            $payloads['timeout'] = 100; // default timeout 100ms
        }

        $header = $this->requestHeader('kafka-php', 0, self::PRODUCE_REQUEST);
        $data   = self::pack(self::BIT_B16, $payloads['required_ack']);
        $data  .= self::pack(self::BIT_B32, $payloads['timeout']);
        $data  .= self::encodeArray($payloads['data'], array($this, '_encodeProcudeTopic'), $compression);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    // }}}
    // {{{ public function metadataRequest()

    /**
     * build metadata request protocol
     *
     * @param array $topics
     * @access public
     * @return string
     */
    public function metadataRequest($topics)
    {
        if (!is_array($topics)) {
            $topics = array($topics);
        }

        foreach ($topics as $topic) {
            if (!is_string($topic)) {
                throw new \Kafka\Exception\Protocol('request metadata topic array have invalid value. ');
            }
        }

        $header = $this->requestHeader('kafka-php', 0, self::METADATA_REQUEST);
        $data   = self::encodeArray($topics, array(__CLASS__, 'encodeString'), self::PACK_INT16);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    // }}}
    // {{{ public function fetchRequest()

    /**
     * build fetch request
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function fetchRequest($payloads)
    {
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception\Protocol('given fetch kafka data invalid. `data` is undefined.');
        }

        if (!isset($payloads['replica_id'])) {
            $payloads['replica_id'] = -1;
        }

        if (!isset($payloads['max_wait_time'])) {
            $payloads['max_wait_time'] = 100; // default timeout 100ms
        }

        if (!isset($payloads['min_bytes'])) {
            $payloads['min_bytes'] = 64 * 1024; // 64k
        }

        $header = $this->requestHeader('kafka-php', 0, self::FETCH_REQUEST);
        $data   = self::pack(self::BIT_B32, $payloads['replica_id']);
        $data  .= self::pack(self::BIT_B32, $payloads['max_wait_time']);
        $data  .= self::pack(self::BIT_B32, $payloads['min_bytes']);
        $data  .= self::encodeArray($payloads['data'], array(__CLASS__, '_encodeFetchTopic'));
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    // }}}
    // {{{ public function offsetRequest()

    /**
     * build offset request
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function offsetRequest($payloads)
    {
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `data` is undefined.');
        }

        if (!isset($payloads['replica_id'])) {
            $payloads['replica_id'] = -1;
        }

        $header = $this->requestHeader('kafka-php', 0, self::OFFSET_REQUEST);
        $data   = self::pack(self::BIT_B32, $payloads['replica_id']);
        $data  .= self::encodeArray($payloads['data'], array($this, '_encodeOffsetTopic'));
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    // }}}
    // {{{ public function groupRequest()

    /**
     * build group request
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function groupRequest($payloads)
    {
        if (!isset($payloads['group_id'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `group_id` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', 0, self::GROUP_COORDINATOR_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    // }}}
    // {{{ public function joinGroupRequest()

    /**
     * build join group request
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function joinGroupRequest($payloads)
    {
        if (!isset($payloads['group_id'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `group_id` is undefined.');
        }
        if (!isset($payloads['session_timeout'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `session_timeout` is undefined.');
        }
        if (!isset($payloads['member_id'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `member_id` is undefined.');
        }
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `data` is undefined.');
        }
        if (!isset($payloads['protocol_type'])) {
            $payloads['protocol_type'] = 'consumer';
        }
        if (!isset($payloads['rebalance_timeout'])) {
            $payloads['rebalance_timeout'] = $payloads['session_timeout'];
        }

        $header = $this->requestHeader('kafka-php', 0, self::JOIN_GROUP_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data   .= self::pack(self::BIT_B32, $payloads['session_timeout']);
        if ($this->getApiVersion(self::JOIN_GROUP_REQUEST) == self::API_VERSION1) {
            $data .= self::pack(self::BIT_B32, $payloads['rebalance_timeout']);
        }
        $data .= self::encodeString($payloads['member_id'], self::PACK_INT16);
        $data .= self::encodeString($payloads['protocol_type'], self::PACK_INT16);
        $data .= self::encodeArray($payloads['data'], array($this, '_encodeGroupProtocol'));
        $data = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    // }}}
    // {{{ public function syncGroupRequest()

    /**
     * build sync group request
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function syncGroupRequest($payloads)
    {
        if (!isset($payloads['group_id'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `group_id` is undefined.');
        }
        if (!isset($payloads['generation_id'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `generation_id` is undefined.');
        }
        if (!isset($payloads['member_id'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `member_id` is undefined.');
        }
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `data` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', 0, self::SYNC_GROUP_REQUEST);
        $data  = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data .= self::pack(self::BIT_B32, $payloads['generation_id']);
        $data .= self::encodeString($payloads['member_id'], self::PACK_INT16);
        $data .= self::encodeArray($payloads['data'], array($this, '_encodeGroupAssignment'));

        $data = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    // }}}
    
    // {{{ public function commitOffsetRequest()

    /**
     * build consumer commit offset request
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function commitOffsetRequest($payloads)
    {
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception\Protocol('given commit offset data invalid. `data` is undefined.');
        }

        if (!isset($payloads['group_id'])) {
            throw new \Kafka\Exception\Protocol('given commit offset data invalid. `group_id` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', 0, self::OFFSET_COMMIT_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data  .= self::encodeArray($payloads['data'], array(__CLASS__, '_encodeCommitOffset'));
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    // }}}
    // {{{ public function fetchOffsetRequest()

    /**
     * build consumer fetch offset request
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function fetchOffsetRequest($payloads)
    {
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception\Protocol('given fetch offset data invalid. `data` is undefined.');
        }

        if (!isset($payloads['group_id'])) {
            throw new \Kafka\Exception\Protocol('given fetch offset data invalid. `group_id` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', 0, self::OFFSET_FETCH_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data  .= self::encodeArray($payloads['data'], array(__CLASS__, '_encodeFetchOffset'));
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $this->stream->write($data);
    }

    // }}}
    // {{{ public static function encodeString()

    /**
     * encode pack string type
     *
     * @param string $string
     * @param int $bytes self::PACK_INT32: int32 big endian order. self::PACK_INT16: int16 big endian order.
     * @param int $compression
     * @return string
     * @static
     * @access public
     */
    public static function encodeString($string, $bytes, $compression = self::COMPRESSION_NONE)
    {
        $packLen = ($bytes == self::PACK_INT32) ? self::BIT_B32 : self::BIT_B16;
        switch ($compression) {
            case self::COMPRESSION_NONE:
                break;
            case self::COMPRESSION_GZIP:
                $string = \gzencode($string);
                break;
            case self::COMPRESSION_SNAPPY:
                // todo
                throw new \Kafka\Exception\NotSupported('SNAPPY compression not yet implemented');
            default:
                throw new \Kafka\Exception\NotSupported('Unknown compression flag: ' . $compression);
        }
        return self::pack($packLen, strlen($string)) . $string;
    }

    // }}}
    // {{{ public static function encodeArray()

    /**
     * encode key array
     *
     * @param array $array
     * @param Callable $func
     * @param null $options
     * @return string
     * @static
     * @access public
     */
    public static function encodeArray(array $array, $func, $options = null)
    {
        if (!is_callable($func, false)) {
            throw new \Kafka\Exception\Protocol('Encode array failed, given function is not callable.');
        }

        $arrayCount = count($array);

        $body = '';
        foreach ($array as $value) {
            if (!is_null($options)) {
                $body .= call_user_func($func, $value, $options);
            } else {
                $body .= call_user_func($func, $value);
            }
        }

        return self::pack(self::BIT_B32, $arrayCount) . $body;
    }

    // }}}
    // {{{ public static function encodeMessageSet()

    /**
     * encode message set
     * N.B., MessageSets are not preceded by an int32 like other array elements
     * in the protocol.
     *
     * @param array $messages
     * @param int $compression
     * @return string
     * @static
     * @access public
     */
    public function encodeMessageSet($messages, $compression = self::COMPRESSION_NONE)
    {
        if (!is_array($messages)) {
            $messages = array($messages);
        }

        $data = '';
        $next = 0;
        foreach ($messages as $message) {
            $tmpMessage = $this->_encodeMessage($message, $compression);

            // int64 -- message offset     Message
            //This is the offset used in kafka as the log sequence number. When the producer is sending non compressed messages, it can set the offsets to anything. When the producer is sending compressed messages, to avoid server side recompression, each compressed message should have offset starting from 0 and increasing by one for each inner message in the compressed message. (see more details about compressed messages in Kafka below)
            $data .= self::pack(self::BIT_B64, $next) . self::encodeString($tmpMessage, self::PACK_INT32);
            $next++;
        }
        return $data;
    }

    // }}}
    // {{{ public function requestHeader()

    /**
     * get request header
     *
     * @param string $clientId
     * @param integer $correlationId
     * @param integer $apiKey
     * @access public
     * @return string
     */
    public function requestHeader($clientId, $correlationId, $apiKey)
    {
        // int16 -- apiKey int16 -- apiVersion int32 correlationId
        $binData  = self::pack(self::BIT_B16, $apiKey);
        $binData .= self::pack(self::BIT_B16, $this->getApiVersion($apiKey));
        $binData .= self::pack(self::BIT_B32, $correlationId);

        // concat client id
        $binData .= self::encodeString($clientId, self::PACK_INT16);
        $msg = sprintf('ClientId: %s ApiKey: %s  ApiVersion: %s', $clientId, self::getApiText($apiKey), $this->getApiVersion($apiKey));
        $this->debug('Start Request ' . $msg);

        return $binData;
    }

    // }}}
    // {{{ protected function _encodeMessage()

    /**
     * encode signal message
     *
     * @param string $message
     * @param int $compression
     * @return string
     * @static
     * @access protected
     */
    protected function _encodeMessage($message, $compression = self::COMPRESSION_NONE)
    {
        // int8 -- magic  int8 -- attribute
        $version = $this->getApiVersion(self::PRODUCE_REQUEST);
        $magic = ($version == self::API_VERSION2) ? self::MESSAGE_MAGIC_VERSION0 : self::MESSAGE_MAGIC_VERSION1;
        $data  = self::pack(self::BIT_B8, $magic);
        $data .= self::pack(self::BIT_B8, $compression);

        // message key
        $data .= self::encodeString('', self::PACK_INT32);

        // message value
        $data .= self::encodeString($message, self::PACK_INT32, $compression);

        $crc = crc32($data);

        // int32 -- crc code  string data
        $message = self::pack(self::BIT_B32, $crc) . $data;

        return $message;
    }

    // }}}
    // {{{ protected static function _encodeProcudePartion()

    /**
     * encode signal part
     *
     * @param $values
     * @param $compression
     * @return string
     * @internal param $partions
     * @static
     * @access protected
     */
    protected function _encodeProcudePartion($values, $compression)
    {
        if (!isset($values['partition_id'])) {
            throw new \Kafka\Exception\Protocol('given produce data invalid. `partition_id` is undefined.');
        }

        if (!isset($values['messages']) || empty($values['messages'])) {
            throw new \Kafka\Exception\Protocol('given produce data invalid. `messages` is undefined.');
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);
        $data .= self::encodeString($this->encodeMessageSet($values['messages'], $compression), self::PACK_INT32);

        return $data;
    }

    // }}}
    // {{{ protected static function _encodeProcudeTopic()

    /**
     * encode signal topic
     *
     * @param $values
     * @param $compression
     * @return string
     * @internal param $partions
     * @static
     * @access protected
     */
    protected function _encodeProcudeTopic($values, $compression)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception\Protocol('given produce data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception\Protocol('given produce data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], array($this, '_encodeProcudePartion'), $compression);

        return $topic . $partitions;
    }

    // }}}
    // {{{ protected static function _encodeFetchPartion()

    /**
     * encode signal part
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeFetchPartion($values)
    {
        if (!isset($values['partition_id'])) {
            throw new \Kafka\Exception\Protocol('given fetch data invalid. `partition_id` is undefined.');
        }

        if (!isset($values['offset'])) {
            $values['offset'] = 0;
        }

        if (!isset($values['max_bytes'])) {
            $values['max_bytes'] = 2 * 1024 * 1024;
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);
        $data .= self::pack(self::BIT_B64, $values['offset']);
        $data .= self::pack(self::BIT_B32, $values['max_bytes']);

        return $data;
    }

    // }}}
    // {{{ protected static function _encodeFetchTopic()

    /**
     * encode signal topic
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeFetchTopic($values)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception\Protocol('given fetch data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception\Protocol('given fetch data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], array(__CLASS__, '_encodeFetchPartion'));

        return $topic . $partitions;
    }

    // }}}

    // {{{ protected function _encodeOffsetPartion()

    /**
     * encode signal part
     *
     * @param partions
     * @access protected
     * @return string
     */
    protected function _encodeOffsetPartion($values)
    {
        if (!isset($values['partition_id'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `partition_id` is undefined.');
        }

        if (!isset($values['time'])) {
            $values['time'] = -1; // -1
        }

        if (!isset($values['max_offset'])) {
            $values['max_offset'] = 100000;
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);
        $data .= self::pack(self::BIT_B64, $values['time']);

        if ($this->getApiVersion(self::OFFSET_REQUEST) == self::API_VERSION0) {
            $data .= self::pack(self::BIT_B32, $values['max_offset']);
        }

        return $data;
    }

    // }}}
    // {{{ protected function _encodeOffsetTopic()

    /**
     * encode signal topic
     *
     * @param partions
     * @access protected
     * @return string
     */
    protected function _encodeOffsetTopic($values)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], array($this, '_encodeOffsetPartion'));

        return $topic . $partitions;
    }

    // }}}
    
    // {{{ protected function _encodeGroupProtocol()

    /**
     * encode group protocol
     *
     * @param partions
     * @access protected
     * @return string
     */
    protected function _encodeGroupProtocol($values)
    {
        if (!isset($values['protocol_name'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `protocol_name` is undefined.');
        }

        $protocolName = self::encodeString($values['protocol_name'], self::PACK_INT16);

        if (!isset($values['version'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `version` is undefined.');
        }

        if (!isset($values['subscription']) || empty($values['subscription'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `subscription` is undefined.');
        }
        if (!isset($values['user_data'])) {
            $values['user_data'] = '';
        }

        $data = self::pack(self::BIT_B16, 0);
        $data .= self::encodeArray($values['subscription'], array($this, '_encodeGroupProtocolMetaTopic'));
        $data .= self::encodeString($values['user_data'], self::PACK_INT32);

        return $protocolName . self::encodeString($data, self::PACK_INT32);
    }

    // }}}
    // {{{ protected function _encodeGroupProtocolMetaTopic()

    /**
     * encode group protocol metadata topic
     *
     * @param partions
     * @access protected
     * @return string
     */
    protected function _encodeGroupProtocolMetaTopic($values)
    {
        $topic = self::encodeString($values, self::PACK_INT16);
        return $topic;
    }

    // }}}

    // {{{ protected function _encodeGroupAssignment()

    /**
     * encode group assignment protocol
     *
     * @param partions
     * @access protected
     * @return string
     */
    protected function _encodeGroupAssignment($values)
    {
        if (!isset($values['version'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `version` is undefined.');
        }
        if (!isset($values['member_id'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `member_id` is undefined.');
        }

        if (!isset($values['assignments']) || empty($values['assignments'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `assignments` is undefined.');
        }
        if (!isset($values['user_data'])) {
            $values['user_data'] = '';
        }

        $memberId = self::encodeString($values['member_id'], self::PACK_INT16);

        $data = self::pack(self::BIT_B16, 0);
        $data .= self::encodeArray($values['assignments'], array($this, '_encodeGroupAssignmentTopic'));
        $data .= self::encodeString($values['user_data'], self::PACK_INT32);

        return $memberId . self::encodeString($data, self::PACK_INT32);
    }

    // }}}
    // {{{ protected function _encodeGroupAssignmentTopic()

    /**
     * encode group assignment topic protocol
     *
     * @param partions
     * @access protected
     * @return string
     */
    protected function _encodeGroupAssignmentTopic($values)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `topic_name` is undefined.');
        }
        if (!isset($values['partitions'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `partitions` is undefined.');
        }

        $topicName = self::encodeString($values['member_id'], self::PACK_INT16);

        $partitions .= self::encodeArray($values['partitions'], array($this, '_encodeGroupAssignmentTopicPartition'));

        return $topicName . $partitions;
    }

    // }}}
    // {{{ protected function _encodeGroupAssignmentTopicPartition()

    /**
     * encode group assignment topic protocol
     *
     * @param partions
     * @access protected
     * @return string
     */
    protected function _encodeGroupAssignmentTopicPartition($values)
    {
        return self::pack(self::BIT_B32, $values);
    }

    // }}}

    // {{{ protected static function _encodeCommitOffsetPartion()

    /**
     * encode signal part
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeCommitOffsetPartion($values)
    {
        if (!isset($values['partition_id'])) {
            throw new \Kafka\Exception\Protocol('given commit offset data invalid. `partition_id` is undefined.');
        }

        if (!isset($values['offset'])) {
            throw new \Kafka\Exception\Protocol('given commit offset data invalid. `offset` is undefined.');
        }

        if (!isset($values['time'])) {
            $values['time'] = -1;
        }

        if (!isset($values['metadata'])) {
            $values['metadata'] = 'm';
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);
        $data .= self::pack(self::BIT_B64, $values['offset']);
        $data .= self::pack(self::BIT_B64, $values['time']);
        $data .= self::encodeString($values['metadata'], self::PACK_INT16);

        return $data;
    }

    // }}}
    // {{{ protected static function _encodeCommitOffset()

    /**
     * encode signal topic
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeCommitOffset($values)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception\Protocol('given commit offset data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception\Protocol('given commit offset data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], array(__CLASS__, '_encodeCommitOffsetPartion'));

        return $topic . $partitions;
    }

    // }}}
    // {{{ protected static function _encodeFetchOffsetPartion()

    /**
     * encode signal part
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeFetchOffsetPartion($values)
    {
        if (!isset($values['partition_id'])) {
            throw new \Kafka\Exception\Protocol('given fetch offset data invalid. `partition_id` is undefined.');
        }

        $data = self::pack(self::BIT_B32, $values['partition_id']);

        return $data;
    }

    // }}}
    // {{{ protected static function _encodeFetchOffset()

    /**
     * encode signal topic
     *
     * @param partions
     * @static
     * @access protected
     * @return string
     */
    protected static function _encodeFetchOffset($values)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception\Protocol('given fetch offset data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception\Protocol('given fetch offset data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], array(__CLASS__, '_encodeFetchOffsetPartion'));

        return $topic . $partitions;
    }

    // }}}
    // }}}
}
