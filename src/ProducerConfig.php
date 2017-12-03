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

namespace Kafka;

/**
+------------------------------------------------------------------------------
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class ProducerConfig extends Config
{
    use SingletonTrait;
    // {{{ consts
    // }}}
    // {{{ members

    protected static $defaults = array(
        'requiredAck' => 1,
        'timeout' => 5000,
        'isAsyn' => false,
        'requestTimeout' => 6000,
        'produceInterval' => 100,
    );

    // }}}
    // {{{ functions
    // {{{ public function setRequestTimeout()

    public function setRequestTimeout($requestTimeout)
    {
        if (!is_numeric($requestTimeout) || $requestTimeout < 1 || $requestTimeout > 900000) {
            throw new \Kafka\Exception\Config('Set request timeout value is invalid, must set it 1 .. 900000');
        }
        static::$options['requestTimeout'] = $requestTimeout;
    }

    // }}}
    // {{{ public function setProduceInterval()

    public function setProduceInterval($produceInterval)
    {
        if (!is_numeric($produceInterval) || $produceInterval < 1 || $produceInterval > 900000) {
            throw new \Kafka\Exception\Config('Set produce interval timeout value is invalid, must set it 1 .. 900000');
        }
        static::$options['produceInterval'] = $produceInterval;
    }

    // }}}
    // {{{ public function setTimeout()

    public function setTimeout($timeout)
    {
        if (!is_numeric($timeout) || $timeout < 1 || $timeout > 900000) {
            throw new \Kafka\Exception\Config('Set timeout value is invalid, must set it 1 .. 900000');
        }
        static::$options['timeout'] = $timeout;
    }

    // }}}
    // {{{ public function setRequiredAck()

    public function setRequiredAck($requiredAck)
    {
        if (!is_numeric($requiredAck) || $requiredAck < -1 || $requiredAck > 1000) {
            throw new \Kafka\Exception\Config('Set required ack value is invalid, must set it -1 .. 1000');
        }
        static::$options['requiredAck'] = $requiredAck;
    }

    // }}}
    // {{{ public function setIsAsyn()

    public function setIsAsyn($asyn)
    {
        if (!is_bool($asyn)) {
            throw new \Kafka\Exception\Config('Set isAsyn value is invalid, must set it bool value');
        }
        static::$options['isAsyn'] = $asyn;
    }

    // }}}
    // }}}
}
