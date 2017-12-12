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

namespace KafkaTest\Base\StreamStub;

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

class Stream
{
    // {{{ consts
    // }}}
    // {{{ members
    // }}}
    // {{{ functions
    // {{{ public function open()

    public function open($path, $mode, $options)
    {
        return true;
    }

    // }}}
    // {{{ public function context()

    public function context($context)
    {
        return true;
    }

    // }}}
    // {{{ public function eof()

    public function eof()
    {
        return false;
    }

    // }}}
    // {{{ public function read()

    public function read($length)
    {
        return false;
    }

    // }}}
    // {{{ public function write()

    public function write($data)
    {
        return strlen($data);
    }

    // }}}
    // {{{ public function metadata()

    public function metadata($path, $option, $var)
    {
        return false;
    }

    // }}}
    // {{{ public function option()

    public function option($option, $args1, $args2)
    {
        return true;
    }

    // }}}
    // }}}
}
