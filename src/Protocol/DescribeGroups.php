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
* Kafka protocol for describe group api
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class DescribeGroups extends Protocol
{
    // {{{ functions
    // {{{ public function encode()

    /**
     * describe group request encode
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function encode($payloads)
    {
        if (!is_array($payloads)) {
            $payloads = array($payloads);
        }

        $header = $this->requestHeader('kafka-php', self::DESCRIBE_GROUPS_REQUEST, self::DESCRIBE_GROUPS_REQUEST);
        $data = self::encodeArray($payloads, array($this, 'encodeString'), self::PACK_INT16);

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
        $groups = $this->decodeArray(substr($data, $offset), array($this, 'describeGroup'));
        $offset += $groups['length'];

        return $groups['data'];
    }

    // }}}
    // {{{ protected function describeGroup()

    /**
     * decode describe group response
     *
     * @access protected
     * @return array
     */
    protected function describeGroup($data)
    {
        $offset = 0;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $groupId = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $groupId['length'];
        $state = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $state['length'];
        $protocolType = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $protocolType['length'];
        $protocol = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $protocol['length'];

        $members = $this->decodeArray(substr($data, $offset), array($this, 'describeMember'));
        $offset += $members['length'];

        return array(
            'length' => $offset,
            'data' => array(
                'errorCode' => $errorCode,
                'groupId' => $groupId['data'],
                'state' => $state['data'],
                'protocolType' => $protocolType['data'],
                'protocol' => $protocol['data'],
                'members' => $members['data']
            )
        );
    }

    // }}}
    // {{{ protected function describeMember()

    /**
     * decode describe members response
     *
     * @access protected
     * @return array
     */
    protected function describeMember($data)
    {
        $offset = 0;
        $memberId = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $memberId['length'];
        $clientId = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $clientId['length'];
        $clientHost = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $clientHost['length'];
        $metadata = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset += $metadata['length'];
        $assignment = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset += $assignment['length'];

        $memberAssignment = $assignment['data'];
        $memberAssignmentOffset = 0;
        $version = self::unpack(self::BIT_B16_SIGNED, substr($memberAssignment, $memberAssignmentOffset, 2));
        $memberAssignmentOffset += 2;
        $partitionAssignments = $this->decodeArray(substr($memberAssignment, $memberAssignmentOffset),
                                array($this, 'describeResponsePartition'));
        $memberAssignmentOffset += $partitionAssignments['length'];
        $userData = $this->decodeString(substr($memberAssignment, $memberAssignmentOffset), self::BIT_B32);

        $metaData = $metadata['data'];
        $metaOffset = 0;
        $version = self::unpack(self::BIT_B16, substr($metaData, $metaOffset, 2));
        $metaOffset += 2;
        $topics = $this->decodeArray(substr($metaData, $metaOffset), array($this, 'decodeString'), self::BIT_B16);
        $metaOffset += $topics['length'];
        $metaUserData = $this->decodeString(substr($metaData, $metaOffset), self::BIT_B32);


        return array(
            'length' => $offset,
            'data' => array(
                'memberId' => $memberId['data'],
                'clientId' => $clientId['data'],
                'clientHost' => $clientHost['data'],
                'metadata' => array(
                    'version' => $version,
                    'topics'  => $topics['data'],
                    'userData' => $metaUserData['data'],
                ),
                'assignment' => array(
                    'version' => $version,
                    'partitions' => $partitionAssignments['data'],
                    'userData' => $userData['data']
                )
            )
        );
    }

    // }}}
    // {{{ protected function describeResponsePartition()

    /**
     * decode describe group partition response
     *
     * @access protected
     * @return array
     */
    protected function describeResponsePartition($data)
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
