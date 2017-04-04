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
            throw new \Kafka\Exception\Protocol('given offset data invalid. `group_id` is undefined.');
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
            $data .= self::encodeString($payloads['member_id'], self::BIT_B16);
        }
        if ($version == self::API_VERSION2) {
            $data .= self::pack(self::BIT_B32, $payloads['generation_id']);
            $data .= self::encodeString($payloads['member_id'], self::BIT_B16);
            $data .= self::pack(self::BIT_B64, $payloads['retention_time']);
        }

        $data .= self::encodeArray($payloads['data'], array(__CLASS__, 'encodeTopic'));
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
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $coordinatorId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $hosts = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $hosts['length'];
        $coordinatorPort = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;

        return array(
            'errorCode' => $errorCode,
            'coordinatorId' => $coordinatorId,
            'coordinatorHost' => $hosts['data'],
            'coordinatorPort' => $coordinatorPort
        );
    }

    // }}}
    // }}}
}
