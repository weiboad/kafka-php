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
* Kafka protocol for list group api
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class ListGroup extends Protocol
{
    // {{{ functions
    // {{{ public function encode()

    /**
     * list group request encode
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function encode($payloads)
    {
        $header = $this->requestHeader('kafka-php', self::LIST_GROUPS_REQUEST, self::LIST_GROUPS_REQUEST);
        $data = self::encodeString($header, self::PACK_INT32);

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
        $groups = $this->decodeArray(substr($data, $offset), array($this, 'listGroup'));

        return array(
            'errorCode' => $errorCode,
            'groups' => $groups['data'],
        );
    }

    // }}}
    // {{{ protected function listGroup()

    /**
     * decode list group response
     *
     * @access protected
     * @return array
     */
    protected function listGroup($data)
    {
        $offset = 0;
        $groupId = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $groupId['length'];
        $protocolType = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $protocolType['length'];

        return array(
            'length' => $offset,
            'data' => array(
                'groupId' => $groupId['data'],
                'protocolType' => $protocolType['data'],
            )
        );
    }

    // }}}
    // }}}
}
