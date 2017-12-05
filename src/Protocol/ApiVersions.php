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
* Kafka protocol for api versions api
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class ApiVersions extends Protocol
{
    // {{{ functions
    // {{{ public function encode()

    /**
     * meta data request encode
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function encode()
    {
        $header = $this->requestHeader('kafka-php', self::API_VERSIONS_REQUEST, self::API_VERSIONS_REQUEST);
        $data   = self::encodeString($header, self::PACK_INT32);

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
        $offset      = 0;
        $errcode     = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset     += 2;
        $apiVersions = $this->decodeArray(substr($data, $offset), [$this, 'apiVersion']);
        $offset     += $apiVersions['length'];

        return [
            'apiVerions' => $apiVersions['data'],
            'errorCode'  => $errcode,
        ];
    }

    // }}}
    // {{{ protected function apiVersion()

    /**
     * decode api version struct
     *
     * @access protected
     * @return array
     */
    protected function apiVersion($data)
    {
        $offset     = 0;
        $apiKey     = self::unpack(self::BIT_B16, substr($data, $offset, 2));
        $offset    += 2;
        $minVersion = self::unpack(self::BIT_B16, substr($data, $offset, 2));
        $offset    += 2;
        $maxVersion = self::unpack(self::BIT_B16, substr($data, $offset, 2));
        $offset    += 2;

        return [
            'length' => $offset,
            'data' => [
                $apiKey,
                $minVersion,
                $maxVersion
            ],
        ];
    }

    // }}}
    // }}}
}
