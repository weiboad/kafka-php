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
* Kafka protocol for commit offset api
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class CommitOffset extends Protocol
{
    // {{{ functions
    // {{{ public function encode()

    /**
     * commit offset request encode
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function encode($payloads)
    {
        if (!isset($payloads['group_id'])) {
            throw new \Kafka\Exception\Protocol('given commit offset data invalid. `group_id` is undefined.');
        }

        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception\Protocol('given commit data invalid. `data` is undefined.');
        }

        if (!isset($payloads['generation_id'])) {
            $payloads['generation_id'] = -1;
        }

        if (!isset($payloads['member_id'])) {
            $payloads['member_id'] = '';
        }

        if (!isset($payloads['retention_time'])) {
            $payloads['retention_time'] = -1;
        }

        $version = $this->getApiVersion(self::OFFSET_COMMIT_REQUEST);

        $header = $this->requestHeader('kafka-php', self::OFFSET_COMMIT_REQUEST, self::OFFSET_COMMIT_REQUEST);

        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        if ($version == self::API_VERSION1) {
            $data .= self::pack(self::BIT_B32, $payloads['generation_id']);
            $data .= self::encodeString($payloads['member_id'], self::PACK_INT16);
        }
        if ($version == self::API_VERSION2) {
            $data .= self::pack(self::BIT_B32, $payloads['generation_id']);
            $data .= self::encodeString($payloads['member_id'], self::PACK_INT16);
            $data .= self::pack(self::BIT_B64, $payloads['retention_time']);
        }

        $data .= self::encodeArray($payloads['data'], array($this, 'encodeTopic'));
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
        $topics = $this->decodeArray(substr($data, $offset), array($this, 'decodeTopic'), $version);
        $offset += $topics['length'];

        return $topics['data'];
    }

    // }}}
    // {{{ protected function encodeTopic()

    /**
     * encode commit offset topic array
     *
     * @param array $values
     * @access public
     * @return array
     */
    protected function encodeTopic($values)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception\Protocol('given commit offset data invalid. `topic_name` is undefined.');
        }
        if (!isset($values['partitions'])) {
            throw new \Kafka\Exception\Protocol('given commit offset data invalid. `partitions` is undefined.');
        }

        $data  = self::encodeString($values['topic_name'], self::PACK_INT16);
        $data .= self::encodeArray($values['partitions'], array($this, 'encodePartition'));

        return $data;
    }

    // }}}
    // {{{ protected function encodePartition()

    /**
     * encode commit offset partition array
     *
     * @param array $values
     * @access public
     * @return array
     */
    protected function encodePartition($values)
    {
        if (!isset($values['partition'])) {
            throw new \Kafka\Exception\Protocol('given commit offset data invalid. `partition` is undefined.');
        }
        if (!isset($values['offset'])) {
            throw new \Kafka\Exception\Protocol('given commit offset data invalid. `offset` is undefined.');
        }
        if (!isset($values['metadata'])) {
            $values['metadata'] = '';
        }
        if (!isset($values['timestamp'])) {
            $values['timestamp'] = time() * 1000;
        }
        $version = $this->getApiVersion(self::OFFSET_COMMIT_REQUEST);

        $data  = self::pack(self::BIT_B32, $values['partition']);
        $data .= self::pack(self::BIT_B64, $values['offset']);
        if ($version == self::API_VERSION1) {
            $data .= self::pack(self::BIT_B64, $values['timestamp']);
        }
        $data .= self::encodeString($values['metadata'], self::PACK_INT16);

        return $data;
    }

    // }}}
    // {{{ protected function decodeTopic()

    /**
     * decode commit offset topic response
     *
     * @param byte[] $data
     * @param string $version
     * @access protected
     * @return array
     */
    protected function decodeTopic($data, $version)
    {
        $offset = 0;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $topicInfo['length'];

        $partitions = $this->decodeArray(substr($data, $offset), array($this, 'decodePartition'), $version);
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
    // {{{ protected function decodePartition()

    /**
     * decode commit offset partition response
     *
     * @param byte[] $data
     * @param string $version
     * @access protected
     * @return array
     */
    protected function decodePartition($data, $version)
    {
        $offset = 0;

        $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;

        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;


        return array(
            'length' => $offset,
            'data' => array(
                'partition' => $partitionId,
                'errorCode' => $errorCode,
            )
        );
    }

    // }}}
    // }}}
}
