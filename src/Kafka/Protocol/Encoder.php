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
