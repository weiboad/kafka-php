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
* Kafka protocol for heart beat api
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Heartbeat extends Protocol
{
    // {{{ functions
    // {{{ public function encode()

    /**
     * heartbeat request encode
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function encode($payloads)
    {
        if (!isset($payloads['group_id'])) {
            throw new \Kafka\Exception\Protocol('given heartbeat data invalid. `group_id` is undefined.');
        }
        if (!isset($payloads['generation_id'])) {
            throw new \Kafka\Exception\Protocol('given heartbeat data invalid. `generation_id` is undefined.');
        }
        if (!isset($payloads['member_id'])) {
            throw new \Kafka\Exception\Protocol('given heartbeat data invalid. `member_id` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', self::HEART_BEAT_REQUEST, self::HEART_BEAT_REQUEST);
        $data  = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data .= self::pack(self::BIT_B32, $payloads['generation_id']);
        $data .= self::encodeString($payloads['member_id'], self::PACK_INT16);

        $data = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    // }}}
    // {{{ public function decode()

    /**
     * decode heart beat response
     *
     * @access public
     * @return array
     */
    public function decode($data)
    {
        $offset = 0;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;

        return array(
            'errorCode' => $errorCode,
        );
    }

    // }}}
    // }}}
}
