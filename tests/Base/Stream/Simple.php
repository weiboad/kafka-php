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

namespace KafkaTest\Base\Stream;

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

class Simple 
{
    // {{{ consts
    // }}}
    // {{{ members
    
    public $context = null;

    protected static $mock = null;

    // }}}
    // {{{ functions
    // {{{ public function stream_open()

    public function stream_open($path, $mode, $options, &$opened_path)
    {
        if (self::$mock !== null) {
            self::$mock->context($this->context);
            return self::$mock->open($path, $mode, $options);
        }
        return true;
    }

    // }}}
    // {{{ public function stream_eof()

    public function stream_eof()
    {
        if (self::$mock !== null) {
            return self::$mock->eof();
        }
        return false;
    }

    // }}}
    // {{{ public function stream_read()

    public function stream_read($len)
    {
        if (self::$mock !== null) {
            return self::$mock->read($len);
        }
        return true;
    }

    // }}}
    // {{{ public function stream_write()

    public function stream_write($data)
    {
        if (self::$mock !== null) {
            return self::$mock->write($data);
        }
        return true;
    }

    // }}}
    // {{{ public function stream_metadata()

    public function stream_metadata($path, $option, $var)
    {
        if (self::$mock !== null) {
            return self::$mock->metadata($path, $option, $var);
        }
        return true;
    }

    // }}}
    // {{{ public static function setMock()
    
    public static function setMock($mock)
    {
        self::$mock = $mock;
    }

    // }}}
    // }}}
}
