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
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    // {{{ consts

    /**
     *  Default kafka broker verion
     */
    const DEFAULT_BROKER_VERION = '0.10.1.0';

    /**
     *  Kafka server protocol version0
     */
    const API_VERSION0 = 0;

    /**
     *  Kafka server protocol version 1
     */
    const API_VERSION1 = 1;

    /**
     *  Kafka server protocol version 2
     */
    const API_VERSION2 = 2;

    /**
     * use encode message, This is a version id used to allow backwards
     * compatible evolution of the message binary format.
     */
    const MESSAGE_MAGIC_VERSION0 = 0;

    /**
     * use encode message, This is a version id used to allow backwards
     * compatible evolution of the message binary format.
     */
    const MESSAGE_MAGIC_VERSION1 = 1;

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
    const GROUP_COORDINATOR_REQUEST = 10;
    const JOIN_GROUP_REQUEST  = 11;
    const HEART_BEAT_REQUEST  = 12;
    const LEAVE_GROUP_REQUEST = 13;
    const SYNC_GROUP_REQUEST  = 14;
    const DESCRIBE_GROUPS_REQUEST = 15;
    const LIST_GROUPS_REQUEST     = 16;

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
     * kafka broker version
     *
     * @var mixed
     * @access protected
     */
    protected $version = self::DEFAULT_BROKER_VERION;

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
    public function __construct(\Kafka\Socket $stream, $version = self::DEFAULT_BROKER_VERION)
    {
        $this->stream = $stream;
        $this->version = $version;
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
        $result = array();
        self::checkLen($type, $bytes);
        if ($type == self::BIT_B64) {
            $set = unpack($type, $bytes);
            $result = ($set[1] & 0xFFFFFFFF) << 32 | ($set[2] & 0xFFFFFFFF);
        } elseif ($type == self::BIT_B16_SIGNED) {
            // According to PHP docs: 's' = signed short (always 16 bit, machine byte order)
            // So lets unpack it..
            $set = unpack($type, $bytes);

            // But if our system is little endian
            if (self::isSystemLittleEndian()) {
                // We need to flip the endianess because coming from kafka it is big endian
                $set = self::convertSignedShortFromLittleEndianToBigEndian($set);
            }
            $result = $set;
        } else {
            $result = unpack($type, $bytes);
        }

        return is_array($result) ? array_shift($result) : $result;
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
        switch ($type) {
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
            list($endiantest) = array_values(unpack('L1L', pack('V', 1)));
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
    // {{{ public function getApiVersion()

    /**
     * Get kafka api version according to specifiy kafka broker version
     *
     * @param int kafka api key
     * @access public
     * @return int
     */
    public function getApiVersion($apikey)
    {
        switch ($apikey) {
            case self::METADATA_REQUEST:
                return self::API_VERSION0;
            case self::PRODUCE_REQUEST:
                if (version_compare($this->version, '0.10.0') >= 0) {
                    return self::API_VERSION2;
                } else if (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION1; 
                } else {
                    return self::API_VERSION0;
                }
            case self::FETCH_REQUEST:
                if (version_compare($this->version, '0.10.0') >= 0) {
                    return self::API_VERSION2;
                } else if (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION1; 
                } else {
                    return self::API_VERSION0;
                }
            case self::OFFSET_REQUEST:
                // todo
                return self::API_VERSION0;
                if (version_compare($this->version, '0.10.1.0') >= 0) {
                    return self::API_VERSION1;
                } else {
                    return self::API_VERSION0;
                }
            case self::GROUP_COORDINATOR_REQUEST:
                return self::API_VERSION0;
            case self::OFFSET_COMMIT_REQUEST:
                if (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION2;
                } else if (version_compare($this->version, '0.8.2') >= 0) {
                    return self::API_VERSION1; 
                } else {
                    return self::API_VERSION0; // supported in 0.8.1 or later
                }
            case self::OFFSET_FETCH_REQUEST:
                if (version_compare($this->version, '0.8.2') >= 0) {
                    return self::API_VERSION1; // Offset Fetch Request v1 will fetch offset from Kafka
                } else {
                    return self::API_VERSION0;//Offset Fetch Request v0 will fetch offset from zookeeper
                }
            case self::JOIN_GROUP_REQUEST:
                if (version_compare($this->version, '0.10.1.0') >= 0) {
                    return self::API_VERSION1;
                } else {
                    return self::API_VERSION0; // supported in 0.9.0.0 and greater
                }
            case self::SYNC_GROUP_REQUEST:
                return self::API_VERSION0;
            case self::HEART_BEAT_REQUEST:
                return self::API_VERSION0;
            case self::LEAVE_GROUP_REQUEST:
                return self::API_VERSION0;
            case self::LIST_GROUPS_REQUEST:
                return self::API_VERSION0;
            case self::DESCRIBE_GROUPS_REQUEST:
                return self::API_VERSION0;
        }

        // default
        return self::API_VERSION0;
    }

    // }}}
    // {{{ public static function getApiText()

    /**
     * Get kafka api text
     *
     * @param int kafka api key
     * @access public
     * @return string
     */
    public static function getApiText($apikey)
    {
        $apis = array(
            self::PRODUCE_REQUEST => 'ProduceRequest',
            self::FETCH_REQUEST   => 'FetchRequest',
            self::OFFSET_REQUEST  => 'OffsetRequest',
            self::METADATA_REQUEST => 'MetadataRequest',
            self::OFFSET_COMMIT_REQUEST => 'OffsetCommitRequest',
            self::OFFSET_FETCH_REQUEST  => 'OffsetFetchRequest',
            self::GROUP_COORDINATOR_REQUEST => 'GroupCoordinatorRequest',
            self::JOIN_GROUP_REQUEST => 'JoinGroupRequest',
            self::HEART_BEAT_REQUEST => 'HeartbeatRequest',
            self::LEAVE_GROUP_REQUEST => 'LeaveGroupRequest',
            self::SYNC_GROUP_REQUEST  => 'SyncGroupRequest',
            self::DESCRIBE_GROUPS_REQUEST => 'DescribeGroupsRequest',
            self::LIST_GROUPS_REQUEST => 'ListGroupsRequest',
        ); 
        return $apis[$apikey];
    }

    // }}}
    // }}}
}
