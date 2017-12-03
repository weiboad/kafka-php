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
* Kafka protocol for sync group api
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class SyncGroup extends Protocol
{
    // {{{ functions
    // {{{ public function encode()

    /**
     * sync group request encode
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function encode($payloads)
    {
        if (!isset($payloads['group_id'])) {
            throw new \Kafka\Exception\Protocol('given sync group data invalid. `group_id` is undefined.');
        }
        if (!isset($payloads['generation_id'])) {
            throw new \Kafka\Exception\Protocol('given sync group data invalid. `generation_id` is undefined.');
        }
        if (!isset($payloads['member_id'])) {
            throw new \Kafka\Exception\Protocol('given sync group data invalid. `member_id` is undefined.');
        }
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception\Protocol('given sync group data invalid. `data` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', self::SYNC_GROUP_REQUEST, self::SYNC_GROUP_REQUEST);
        $data  = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data .= self::pack(self::BIT_B32, $payloads['generation_id']);
        $data .= self::encodeString($payloads['member_id'], self::PACK_INT16);
        $data .= self::encodeArray($payloads['data'], array($this, 'encodeGroupAssignment'));

        $data = self::encodeString($header . $data, self::PACK_INT32);

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
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;

        $memberAssignments = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset += $memberAssignments['length'];

        $memberAssignment = $memberAssignments['data'];
        if (strlen($memberAssignment)) {
            $memberAssignmentOffset = 0;
            $version = self::unpack(self::BIT_B16_SIGNED, substr($memberAssignment, $memberAssignmentOffset, 2));
            $memberAssignmentOffset += 2;
            $partitionAssignments = $this->decodeArray(substr($memberAssignment, $memberAssignmentOffset),
                                    array($this, 'syncGroupResponsePartition'));
            $memberAssignmentOffset += $partitionAssignments['length'];
            $userData = $this->decodeString(substr($memberAssignment, $memberAssignmentOffset), self::BIT_B32);
        } else {
            return array(
                'errorCode' => $errorCode,
            );
        }

        return array(
            'errorCode' => $errorCode,
            'partitionAssignments' => $partitionAssignments['data'],
            'version' => $version,
            'userData' => $userData['data'],
        );
    }

    // }}}
    // {{{ protected function encodeGroupAssignment()

    /**
     * encode group assignment protocol
     *
     * @param partions
     * @access protected
     * @return string
     */
    protected function encodeGroupAssignment($values)
    {
        if (!isset($values['version'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `version` is undefined.');
        }
        if (!isset($values['member_id'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `member_id` is undefined.');
        }

        if (!isset($values['assignments'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `assignments` is undefined.');
        }
        if (!isset($values['user_data'])) {
            $values['user_data'] = '';
        }

        $memberId = self::encodeString($values['member_id'], self::PACK_INT16);

        $data = self::pack(self::BIT_B16, 0);
        $data .= self::encodeArray($values['assignments'], array($this, 'encodeGroupAssignmentTopic'));
        $data .= self::encodeString($values['user_data'], self::PACK_INT32);

        return $memberId . self::encodeString($data, self::PACK_INT32);
    }

    // }}}
    // {{{ protected function encodeGroupAssignmentTopic()

    /**
     * encode group assignment topic protocol
     *
     * @param partions
     * @access protected
     * @return string
     */
    protected function encodeGroupAssignmentTopic($values)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `topic_name` is undefined.');
        }
        if (!isset($values['partitions'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `partitions` is undefined.');
        }

        $topicName = self::encodeString($values['topic_name'], self::PACK_INT16);

        $partitions = self::encodeArray($values['partitions'], array($this, 'encodeGroupAssignmentTopicPartition'));

        return $topicName . $partitions;
    }

    // }}}
    // {{{ protected function encodeGroupAssignmentTopicPartition()

    /**
     * encode group assignment topic protocol
     *
     * @param partions
     * @access protected
     * @return string
     */
    protected function encodeGroupAssignmentTopicPartition($values)
    {
        return self::pack(self::BIT_B32, $values);
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
        $offset += $partitions['length'];

        return array(
            'length' => $offset,
            'data' => array(
                'topicName' => $topicName['data'],
                'partitions' => $partitions['data'],
            )
        );
    }

    // }}}
    // }}}
}
