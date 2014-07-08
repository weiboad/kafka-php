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
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

abstract class Protocol
{
    // {{{ consts

    /**
     *  Kafka server protocol version
     */
    const API_VERSION = 0;

    /**
     * use encode message, This is a version id used to allow backwards
     * compatible evolution of the message binary format.
     */
    const MESSAGE_MAGIC = 0;

    /**
     * message no compression
     */
    const COMPRESSION_NONE = 0;

    /**
     * Message using gzip compression
     */
    const COMPRESSION_GZIP = 1;

    /**
     * Message using Snappy compression
     */
    const COMPRESSION_SNAPPY = 2;

    /**
     *  pack int32 type
     */
    const PACK_INT32 = 0;

    /**
     * pack int16 type
     */
    const PACK_INT16 = 1;

    /**
     * protocol request code
     */
    const PRODUCE_REQUEST = 0;
    const FETCH_REQUEST   = 1;
    const OFFSET_REQUEST  = 2;
    const METADATA_REQUEST      = 3;
    const OFFSET_COMMIT_REQUEST = 8;
    const OFFSET_FETCH_REQUEST  = 9;
    const CONSUMER_METADATA_REQUEST = 10;

    // }}}
    // {{{ members

    /**
     * stream
     *
     * @var mixed
     * @access protected
     */
    protected $stream = null;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    /**
     * __construct
     *
     * @param \Kafka\Socket $stream
     * @access public
     * @return void
     */
    public function __construct(\Kafka\Socket $stream)
    {
        $this->stream = $stream;
    }

    // }}}
    // {{{ public static function packInt64()

    /**
     * Pack a 64bit integer as big endian long
     *
     * @static
     * @access public
     * @return integer
     */
    public static function packInt64($big)
    {
        if ($big == -1) { // -1L
            $data = hex2bin('ffffffffffffffff');
        } elseif ($big == -2) { // -2L
            $data = hex2bin('fffffffffffffffe');
        } else {
            $left  = 0xffffffff00000000;
            $right = 0x00000000ffffffff;

            $l = ($big & $left) >> 32;
            $r = $big & $right;
            $data = pack('NN', $l, $r);
        }

        return $data;
    }

    // }}}
    // {{{ public static function unpackInt64()

    /**
     * Unpack a 64bit integer as big endian long
     *
     * @static
     * @access public
     * @return integer
     */
    public static function unpackInt64($bytes)
    {
        $set = unpack('N2', $bytes);
        return $original = ($set[1] & 0xFFFFFFFF) << 32 | ($set[2] & 0xFFFFFFFF);
    }

    // }}}
    // }}}
}
