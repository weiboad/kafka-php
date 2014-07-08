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

namespace KafkaMock\Protocol;

use \Kafka\Protocol\Encoder as KEncoder;

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

class Encoder extends KEncoder
{
    // {{{ consts
    // }}}
    // {{{ members
    // }}}
    // {{{ functions
    // {{{ public static function encodeMessage()

    /**
     * encodeMessage
     *
     * @access public
     * @return void
     */
    public static function encodeMessage($message, $compression = self::COMPRESSION_NONE)
    {
        return parent::_encodeMessage($message, $compression);
    }

    // }}}
    // {{{ public static function encodeProcudePartion()

    /**
     * encodeProcudePartion
     *
     * @access public
     * @return void
     */
    public static function encodeProcudePartion($values, $compression)
    {
        return parent::_encodeProcudePartion($values, $compression);
    }

    // }}}
    // {{{ public static function encodeProcudeTopic()

    /**
     * encodeProcudeTopic
     *
     * @access public
     * @return void
     */
    public static function encodeProcudeTopic($values, $compression)
    {
        return parent::_encodeProcudeTopic($values, $compression);
    }

    // }}}
    // }}}
}
