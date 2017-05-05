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
* Kafka protocol for leave group api
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class LeaveGroup extends Protocol
{
    // {{{ functions
    // {{{ public function encode()

    /**
     * leave group request encode
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function encode($payloads)
    {
        if (!isset($payloads['group_id'])) {
            throw new \Kafka\Exception\Protocol('given leave group data invalid. `group_id` is undefined.');
        }
        if (!isset($payloads['member_id'])) {
            throw new \Kafka\Exception\Protocol('given leave group data invalid. `member_id` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', self::LEAVE_GROUP_REQUEST, self::LEAVE_GROUP_REQUEST);
        $data  = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data .= self::encodeString($payloads['member_id'], self::PACK_INT16);

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
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, 0, 2));

        return array(
            'errorCode' => $errorCode,
        );
    }

    // }}}
    // }}}
}
