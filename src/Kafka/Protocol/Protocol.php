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

    // unpack/pack bit
    const BIT_B64 = 'N2';
    const BIT_B32 = 'N';
    const BIT_B16 = 'n';
    const BIT_B16_SIGNED = 's';
    const BIT_B8  = 'C';

    // }}}
    // {{{ members

    /**
     * stream
     *
     * @var mixed
     * @access protected
     */
    protected $stream = null;

    /**
     * isBigEndianSystem
     *
     * gets set to true if the computer this code is running is little endian,
     * gets set to false if the computer this code is running on is big endian.
     *
     * @var null|bool
     * @access private
     */
    private static $isLittleEndianSystem = null;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    /**
     * __construct
     *
     * @param \Kafka\Socket $stream
     * @access public
     */
    public function __construct(\Kafka\Socket $stream)
    {
        $this->stream = $stream;
    }

    // }}}
    // {{{ public static function Khex2bin()

    /**
     * hex to bin
     *
     * @param string $string
     * @static
     * @access protected
     * @return string (raw)
     */
    public static function Khex2bin($string)
    {
        if (function_exists('\hex2bin')) {
            return \hex2bin($string);
        } else {
            $bin = '';
            $len = strlen($string);
            for ($i = 0; $i < $len; $i += 2) {
                $bin .= pack('H*', substr($string, $i, 2));
            }

            return $bin;
        }
    }

    // }}}
    // {{{ public static function unpack()

    /**
     * Unpack a bit integer as big endian long
     *
     * @static
     * @access public
     * @param $type
     * @param $bytes
     * @return int
     */
    public static function unpack($type, $bytes)
    {
        self::checkLen($type, $bytes);
        if ($type == self::BIT_B64) {
            $set = unpack($type, $bytes);
            $original = ($set[1] & 0xFFFFFFFF) << 32 | ($set[2] & 0xFFFFFFFF);
            return $original;
        } elseif ($type == self::BIT_B16_SIGNED) {
            // According to PHP docs: 's' = signed short (always 16 bit, machine byte order)
            // So lets unpack it..
            $set = unpack($type, $bytes);

            // But if our system is little endian
            if (self::isSystemLittleEndian()) {
                // We need to flip the endianess because coming from kafka it is big endian
                $set = self::convertSignedShortFromLittleEndianToBigEndian($set);
            }
            return $set;
        } else {
            return unpack($type, $bytes);
        }
    }

    // }}}
    // {{{ public static function pack()

    /**
     * pack a bit integer as big endian long
     *
     * @static
     * @access public
     * @param $type
     * @param $data
     * @return int
     */
    public static function pack($type, $data)
    {
        if ($type == self::BIT_B64) {
            if ($data == -1) { // -1L
                $data = self::Khex2bin('ffffffffffffffff');
            } elseif ($data == -2) { // -2L
                $data = self::Khex2bin('fffffffffffffffe');
            } else {
                $left  = 0xffffffff00000000;
                $right = 0x00000000ffffffff;

                $l = ($data & $left) >> 32;
                $r = $data & $right;
                $data = pack($type, $l, $r);
            }
        } else {
            $data = pack($type, $data);
        }

        return $data;
    }

    // }}}
    // {{{ protected static function checkLen()

    /**
     * check unpack bit is valid
     *
     * @param string $type
     * @param string(raw) $bytes
     * @static
     * @access protected
     * @return void
     */
    protected static function checkLen($type, $bytes)
    {
        $len = 0;
        switch($type) {
            case self::BIT_B64:
                $len = 8;
                break;
            case self::BIT_B32:
                $len = 4;
                break;
            case self::BIT_B16:
                $len = 2;
                break;
            case self::BIT_B16_SIGNED:
                $len = 2;
                break;
            case self::BIT_B8:
                $len = 1;
                break;
        }

        if (strlen($bytes) != $len) {
            throw new \Kafka\Exception\Protocol('unpack failed. string(raw) length is ' . strlen($bytes) . ' , TO ' . $type);
        }
    }

    // }}}
    // {{{ public static function isSystemLittleEndian()

    /**
     * Determines if the computer currently running this code is big endian or little endian.
     *
     * @access public
     * @return bool - false if big endian, true if little endian
     */
    public static function isSystemLittleEndian()
    {
        // If we don't know if our system is big endian or not yet...
        if (is_null(self::$isLittleEndianSystem)) {
            // Lets find out
            list ($endiantest) = array_values(unpack('L1L', pack('V', 1)));
            if ($endiantest != 1) {
                // This is a big endian system
                self::$isLittleEndianSystem = false;
            } else {
                // This is a little endian system
                self::$isLittleEndianSystem = true;
            }
        }

        return self::$isLittleEndianSystem;
    }

    // }}}
    // {{{ public static function convertSignedShortFromLittleEndianToBigEndian()

    /**
     * Converts a signed short (16 bits) from little endian to big endian.
     *
     * @param int[] $bits
     * @access public
     * @return array
     */
    public static function convertSignedShortFromLittleEndianToBigEndian($bits)
    {
        foreach ($bits as $index => $bit) {

            // get LSB
            $lsb = $bit & 0xff;

            // get MSB
            $msb = $bit >> 8 & 0xff;

            // swap bytes
            $bit = $lsb <<8 | $msb;

            if ($bit >= 32768) {
                $bit -= 65536;
            }
            $bits[$index] = $bit;
        }
        return $bits;
    }

    // }}}
    // }}}
}
