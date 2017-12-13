<?php
namespace Kafka\Protocol;

class LeaveGroup extends Protocol
{

    /**
     * leave group request encode
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function encode($payloads)
    {
        if (! isset($payloads['group_id'])) {
            throw new \Kafka\Exception\Protocol('given leave group data invalid. `group_id` is undefined.');
        }
        if (! isset($payloads['member_id'])) {
            throw new \Kafka\Exception\Protocol('given leave group data invalid. `member_id` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', self::LEAVE_GROUP_REQUEST, self::LEAVE_GROUP_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data  .= self::encodeString($payloads['member_id'], self::PACK_INT16);

        $data = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    /**
     * decode group response
     *
     * @access public
     * @return array
     */
    public function decode($data)
    {
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, 0, 2));

        return [
            'errorCode' => $errorCode,
        ];
    }
}
