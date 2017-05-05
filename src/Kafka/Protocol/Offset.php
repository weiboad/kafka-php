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
* Kafka protocol for offset api
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Offset extends Protocol
{
    // {{{ functions
    // {{{ public function encode()

    /**
     * offset request encode
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function encode($payloads)
    {
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `data` is undefined.');
        }

        if (!isset($payloads['replica_id'])) {
            $payloads['replica_id'] = -1;
        }

        $header = $this->requestHeader('kafka-php', self::OFFSET_REQUEST, self::OFFSET_REQUEST);
        $data   = self::pack(self::BIT_B32, $payloads['replica_id']);
        $data  .= self::encodeArray($payloads['data'], array($this, 'encodeOffsetTopic'));
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    // }}}
    // {{{ public function decode()

    /**
     * decode group response
     *
     * @access public
     * @return array
     */
    public function decode($data)
    {
        $offset = 0;

        $version = $this->getApiVersion(self::OFFSET_REQUEST);
        $topics = $this->decodeArray(substr($data, $offset), array($this, 'offsetTopic'), $version);
        $offset += $topics['length'];
        
        return $topics['data'];
    }

    // }}}
    // {{{ protected function encodeOffsetPartion()

    /**
     * encode signal part
     *
     * @param partions
     * @access protected
     * @return string
     */
    protected function encodeOffsetPartion($values)
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
    // {{{ protected function encodeOffsetTopic()

    /**
     * encode signal topic
     *
     * @param partions
     * @access protected
     * @return string
     */
    protected function encodeOffsetTopic($values)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception\Protocol('given offset data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], array($this, 'encodeOffsetPartion'));

        return $topic . $partitions;
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
    // }}}
}
