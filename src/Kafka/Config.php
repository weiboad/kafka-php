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

abstract class Config
{
    use SingletonTrait;
    // {{{ consts
    // }}}
    // {{{ members

    private $options = array();

    private static $defaults = array(
        'clientId' => 'kafka-php',
        'brokerVersion' => '0.10.1.0',
        'metadataBrokerList' => '',
        'messageMaxBytes' => '1000000',
        'metadataRequestTimeoutMs' => '60000',
        'metadataRefreshIntervalMs' => '300000',
        'metadataMaxAgeMs' => -1,
    );

    // }}}
    // {{{ functions
    // {{{ public function __call()

    public function __call($name, $args)
    {
        if (strpos($name, 'get') === 0) {
            $option = strtolower(substr($name, 3, 1)) . substr($name, 4);
            if (isset($this->options[$option])) {
                return $this->options[$option]; 
            }

            if (isset(self::$defaults[$option])) {
                return self::$defaults[$option];
            }
            if (isset(static::$defaults[$option])) {
                return static::$defaults[$option];
            }
        }

        if (strpos($name, 'set') === 0) {
            if (count($args) != 1) {
                return false;
            }
            $option = strtolower(substr($name, 3, 1)) . substr($name, 4);
            $this->options[$option] = $args[0];
            // check todo
            return true;
        }
    }

    // }}}
    // }}}
}
