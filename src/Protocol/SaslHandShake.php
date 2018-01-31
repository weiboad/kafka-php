<?php
namespace Kafka\Protocol;

class SaslHandShake extends Protocol
{
    
    private static $allowSaslMechanisms = [
        'GSSAPI',
        'PLAIN',
        'SCRAM-SHA-256',
        'SCRAM-SHA-512'
    ];

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
            throw new \Kafka\Exception\Protocol('Invalid request SASL hand shake mechanism given. ');
        }

        if (! in_array($mechanism, self::$allowSaslMechanisms, true)) {
            throw new \Kafka\Exception\Protocol('Invalid request SASL hand shake mechanism given, it must be one of: ' . implode('|', self::$allowSaslMechanisms));
        }

        $header = $this->requestHeader('kafka-php', self::SASL_HAND_SHAKE_REQUEST, self::SASL_HAND_SHAKE_REQUEST);
        $data   = self::encodeString($mechanism, self::PACK_INT16);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    /**
     * decode sasl hand shake response
     *
     * @access public
     * @return array
     */
    public function decode($data)
    {
        $offset            = 0;
        $errcode           = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset           += 2;
        $enabledMechanisms = $this->decodeArray(substr($data, $offset), [$this, 'mechanism']);
        $offset           += $enabledMechanisms['length'];

        return [
            'mechanisms' => $enabledMechanisms['data'],
            'errorCode'  => $errcode,
        ];
    }

    /**
     * decode string
     *
     * @access protected
     * @return array
     */
    protected function mechanism($data)
    {
        $offset        = 0;
        $mechanismInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset       += $mechanismInfo['length'];
        return [
            'length' => $offset,
            'data' => $mechanismInfo['data']
        ];
    }
}
