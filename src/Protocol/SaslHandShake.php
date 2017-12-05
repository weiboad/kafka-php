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
* Kafka protocol for sasl hand shake api
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class SaslHandShake extends Protocol
{
    // {{{ members
    
    protected $allowSaslMechanisms = [
        'GSSAPI',
        'PLAIN',
        'SCRAM-SHA-256',
        'SCRAM-SHA-512'
    ];

    // }}}
    // {{{ functions
    // {{{ public function encode()

    /**
     * meta data request encode
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function encode($mechanism)
    {
        if (! is_string($mechanism)) {
            throw new \Kafka\Exception\Protocol('request sasl hand shake mechanism invalid value. ');
        }

        if (! in_array($mechanism, $this->allowSaslMechanisms)) {
            throw new \Kafka\Exception\Protocol('request sasl hand shake mechanism invalid value, must in' . implode('|', $this->allowSaslMechanisms));
        }

        $header = $this->requestHeader('kafka-php', self::SASL_HAND_SHAKE_REQUEST, self::SASL_HAND_SHAKE_REQUEST);
        $data   = self::encodeString($mechanism, self::PACK_INT16);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    // }}}
    // {{{ public function decode()

    /**
     * decode sasl hand shake response
     *
     * @access public
     * @return array
     */
    public function decode($data)
    {
        $offset       = 0;
        $errcode  = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset  += 2;
        $enabledMechanisms = $this->decodeArray(substr($data, $offset), [$this, 'mechanism']);
        $offset  += $enabledMechanisms['length'];

        $result = [
            'mechanisms' => $enabledMechanisms['data'],
            'errorCode'  => $errcode,
        ];
        return $result;
    }

    // }}}
    // {{{ protected function mechanism()

    /**
     * decode string
     *
     * @access protected
     * @return array
     */
    protected function mechanism($data)
    {
        $offset       = 0;
        $mechanismInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset      += $mechanismInfo['length'];
        return [
            'length' => $offset,
            'data' => $mechanismInfo['data']
        ];
    }

    // }}}
    // }}}
}
