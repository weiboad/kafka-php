<?php

namespace Kafka\Protocol;

abstract class Protocol
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    /**
     *  Default kafka broker verion
     */
    public const DEFAULT_BROKER_VERION = '0.9.0.0';

    /**
     *  Kafka server protocol version0
     */
    public const API_VERSION0 = 0;

    /**
     *  Kafka server protocol version 1
     */
    public const API_VERSION1 = 1;

    /**
     *  Kafka server protocol version 2
     */
    public const API_VERSION2 = 2;

    /**
     * use encode message, This is a version id used to allow backwards
     * compatible evolution of the message binary format.
     */
    public const MESSAGE_MAGIC_VERSION0 = 0;

    /**
     * use encode message, This is a version id used to allow backwards
     * compatible evolution of the message binary format.
     */
    public const MESSAGE_MAGIC_VERSION1 = 1;

    /**
     * message no compression
     */
    public const COMPRESSION_NONE = 0;

    /**
     * Message using gzip compression
     */
    public const COMPRESSION_GZIP = 1;

    /**
     * Message using Snappy compression
     */
    public const COMPRESSION_SNAPPY = 2;

    /**
     *  pack int32 type
     */
    public const PACK_INT32 = 0;

    /**
     * pack int16 type
     */
    public const PACK_INT16 = 1;

    /**
     * protocol request code
     */
    public const PRODUCE_REQUEST = 0;

    public const FETCH_REQUEST = 1;

    public const OFFSET_REQUEST = 2;

    public const METADATA_REQUEST = 3;

    public const OFFSET_COMMIT_REQUEST = 8;

    public const OFFSET_FETCH_REQUEST = 9;

    public const GROUP_COORDINATOR_REQUEST = 10;

    public const JOIN_GROUP_REQUEST = 11;

    public const HEART_BEAT_REQUEST = 12;

    public const LEAVE_GROUP_REQUEST = 13;

    public const SYNC_GROUP_REQUEST = 14;

    public const DESCRIBE_GROUPS_REQUEST = 15;

    public const LIST_GROUPS_REQUEST = 16;

    public const SASL_HAND_SHAKE_REQUEST = 17;

    public const API_VERSIONS_REQUEST = 18;

    public const CREATE_TOPICS_REQUEST = 19;

    public const DELETE_TOPICS_REQUEST = 20;

    // unpack/pack bit
    public const BIT_B64 = 'N2';

    public const BIT_B32 = 'N';

    public const BIT_B16 = 'n';

    public const BIT_B16_SIGNED = 's';

    public const BIT_B8 = 'C';

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

    /**
     * __construct
     *
     * @param string version
     *
     * @access public
     */
    public function __construct(string $version = self::DEFAULT_BROKER_VERION)
    {
        $this->version = $version;
    }

    /**
     * Unpack a bit integer as big endian long
     *
     * @static
     * @access public
     *
     * @param $type
     * @param $bytes
     *
     * @return int
     */
    public static function unpack($type, $bytes)
    {
        $result = [];
        self::checkLen($type, $bytes);
        if ($type == self::BIT_B64) {
            $set    = unpack($type, $bytes);
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

    /**
     * pack a bit integer as big endian long
     *
     * @static
     * @access public
     *
     * @param $type
     * @param $data
     *
     * @return int
     */
    public static function pack($type, $data)
    {
        if ($type == self::BIT_B64) {
            if ($data == -1) { // -1L
                $data = \hex2bin('ffffffffffffffff');
            } elseif ($data == -2) { // -2L
                $data = \hex2bin('fffffffffffffffe');
            } else {
                $left  = 0xffffffff00000000;
                $right = 0x00000000ffffffff;

                $l    = ($data & $left) >> 32;
                $r    = $data & $right;
                $data = pack($type, $l, $r);
            }
        } else {
            $data = pack($type, $data);
        }

        return $data;
    }

    /**
     * check unpack bit is valid
     *
     * @param string $type
     * @param string(raw) $bytes
     *
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

        if (strlen($bytes) !== $len) {
            throw new \Kafka\Exception\Protocol('unpack failed. string(raw) length is ' . strlen($bytes) . ' , TO ' . $type);
        }
    }

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

    /**
     * Converts a signed short (16 bits) from little endian to big endian.
     *
     * @param int[] $bits
     *
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
            $bit = $lsb << 8 | $msb;

            if ($bit >= 32768) {
                $bit -= 65536;
            }
            $bits[$index] = $bit;
        }

        return $bits;
    }

    /**
     * Get kafka api version according to specifiy kafka broker version
     *
     * @param int kafka api key
     *
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
                }

                if (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION1;
                }

                return self::API_VERSION0;
            case self::FETCH_REQUEST:
                if (version_compare($this->version, '0.10.0') >= 0) {
                    return self::API_VERSION2;
                }

                if (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION1;
                }

                return self::API_VERSION0;
            case self::OFFSET_REQUEST:
//                TODO: make it compatible with V1 of OFFSET_REQUEST
//                if (version_compare($this->version, '0.10.1.0') >= 0) {
//                    return self::API_VERSION1;
//                } else {
//                    return self::API_VERSION0;
//                }
                return self::API_VERSION0;
            case self::GROUP_COORDINATOR_REQUEST:
                return self::API_VERSION0;
            case self::OFFSET_COMMIT_REQUEST:
                if (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION2;
                }

                if (version_compare($this->version, '0.8.2') >= 0) {
                    return self::API_VERSION1;
                }

                return self::API_VERSION0; // supported in 0.8.1 or later
            case self::OFFSET_FETCH_REQUEST:
                if (version_compare($this->version, '0.8.2') >= 0) {
                    return self::API_VERSION1; // Offset Fetch Request v1 will fetch offset from Kafka
                }

                return self::API_VERSION0;//Offset Fetch Request v0 will fetch offset from zookeeper
            case self::JOIN_GROUP_REQUEST:
                if (version_compare($this->version, '0.10.1.0') >= 0) {
                    return self::API_VERSION1;
                }

                return self::API_VERSION0; // supported in 0.9.0.0 and greater
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

    /**
     * Get kafka api text
     *
     * @param int kafka api key
     *
     * @access public
     * @return string
     */
    public static function getApiText($apikey)
    {
        $apis = [
            self::PRODUCE_REQUEST           => 'ProduceRequest',
            self::FETCH_REQUEST             => 'FetchRequest',
            self::OFFSET_REQUEST            => 'OffsetRequest',
            self::METADATA_REQUEST          => 'MetadataRequest',
            self::OFFSET_COMMIT_REQUEST     => 'OffsetCommitRequest',
            self::OFFSET_FETCH_REQUEST      => 'OffsetFetchRequest',
            self::GROUP_COORDINATOR_REQUEST => 'GroupCoordinatorRequest',
            self::JOIN_GROUP_REQUEST        => 'JoinGroupRequest',
            self::HEART_BEAT_REQUEST        => 'HeartbeatRequest',
            self::LEAVE_GROUP_REQUEST       => 'LeaveGroupRequest',
            self::SYNC_GROUP_REQUEST        => 'SyncGroupRequest',
            self::DESCRIBE_GROUPS_REQUEST   => 'DescribeGroupsRequest',
            self::LIST_GROUPS_REQUEST       => 'ListGroupsRequest',
            self::SASL_HAND_SHAKE_REQUEST   => 'SaslHandShakeRequest',
            self::API_VERSIONS_REQUEST      => 'ApiVersionsRequest',
        ];

        return $apis[$apikey];
    }

    /**
     * get request header
     *
     * @param string  $clientId
     * @param integer $correlationId
     * @param integer $apiKey
     *
     * @access public
     * @return string
     */
    public function requestHeader($clientId, $correlationId, $apiKey)
    {
        // int16 -- apiKey int16 -- apiVersion int32 correlationId
        $binData  = self::pack(self::BIT_B16, $apiKey);
        $binData .= self::pack(self::BIT_B16, $this->getApiVersion($apiKey));
        $binData .= self::pack(self::BIT_B32, $correlationId);

        // concat client id
        $binData .= self::encodeString($clientId, self::PACK_INT16);

        $this->debug(
            sprintf(
                'Start Request ClientId: %s ApiKey: %s  ApiVersion: %s',
                $clientId,
                self::getApiText($apiKey),
                $this->getApiVersion($apiKey)
            )
        );

        return $binData;
    }

    /**
     * @throws \Kafka\Exception\NotSupported
     */
    public static function encodeString(string $string, int $bytes, int $compression = self::COMPRESSION_NONE)
    {
        $packLen = $bytes === self::PACK_INT32 ? self::BIT_B32 : self::BIT_B16;
        $string  = self::compress($string, $compression);

        return self::pack($packLen, \strlen($string)) . $string;
    }

    private static function compress(string $string, int $compression): string
    {
        if ($compression === self::COMPRESSION_NONE) {
            return $string;
        }

        if ($compression === self::COMPRESSION_SNAPPY) {
            throw new \Kafka\Exception\NotSupported('SNAPPY compression not yet implemented');
        }

        if ($compression !== self::COMPRESSION_GZIP) {
            throw new \Kafka\Exception\NotSupported('Unknown compression flag: ' . $compression);
        }

        return \gzencode($string);
    }

    public static function encodeArray(array $array, $func, ?int $options = null)
    {
        if (! is_callable($func, false)) {
            throw new \Kafka\Exception\Protocol('Encode array failed, given function is not callable.');
        }

        $arrayCount = count($array);

        $body = '';
        foreach ($array as $value) {
            $body .= $options !== null ? $func($value, $options) : $func($value);
        }

        return self::pack(self::BIT_B32, $arrayCount) . $body;
    }

    public function decodeString(string $data, string $bytes, int $compression = self::COMPRESSION_NONE): array
    {
        $offset  = $bytes === self::BIT_B32 ? 4 : 2;
        $packLen = self::unpack($bytes, substr($data, 0, $offset)); // int16 topic name length

        if ($packLen === 4294967295) { // uint32(4294967295) is int32 (-1)
            $packLen = 0;
        }

        if ($packLen === 0) {
            return ['length' => $offset, 'data' => ''];
        }

        $data    = (string) substr($data, $offset, $packLen);
        $offset += $packLen;

        return ['length' => $offset, 'data' => self::decompress($data, $compression)];
    }

    private static function decompress(string $string, int $compression): string
    {
        if ($compression === self::COMPRESSION_NONE) {
            return $string;
        }

        if ($compression === self::COMPRESSION_SNAPPY) {
            throw new \Kafka\Exception\NotSupported('SNAPPY compression not yet implemented');
        }

        if ($compression !== self::COMPRESSION_GZIP) {
            throw new \Kafka\Exception\NotSupported('Unknown compression flag: ' . $compression);
        }

        return \gzdecode($string);
    }

    public function decodeArray(string $data, callable $func, $options = null)
    {
        $offset     = 0;
        $arrayCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset    += 4;

        $result = [];

        for ($i = 0; $i < $arrayCount; $i++) {
            $value = substr($data, $offset);
            $ret   = $options !== null ? $func($value, $options) : $func($value);

            if (! \is_array($ret) && $ret === false) {
                break;
            }

            if (! isset($ret['length'], $ret['data'])) {
                throw new \Kafka\Exception\Protocol('Decode array failed, given function return format is invalid');
            }
            if ((int) $ret['length'] === 0) {
                continue;
            }

            $offset  += $ret['length'];
            $result[] = $ret['data'];
        }

        return ['length' => $offset, 'data' => $result];
    }

    public function decodePrimitiveArray(string $data, string $bit): array
    {
        $offset     = 0;
        $arrayCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset    += 4;

        if ($arrayCount === 4294967295) {
            $arrayCount = 0;
        }

        $result = [];

        for ($i = 0; $i < $arrayCount; $i++) {
            if ($bit === self::BIT_B64) {
                $result[] = self::unpack(self::BIT_B64, substr($data, $offset, 8));
                $offset  += 8;
            } elseif ($bit === self::BIT_B32) {
                $result[] = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset  += 4;
            } elseif (\in_array($bit, [self::BIT_B16, self::BIT_B16_SIGNED], true)) {
                $result[] = self::unpack($bit, substr($data, $offset, 2));
                $offset  += 2;
            } elseif ($bit === self::BIT_B8) {
                $result[] = self::unpack($bit, substr($data, $offset, 1));
                ++$offset;
            }
        }

        return ['length' => $offset, 'data' => $result];
    }

    /**
     * @throws \Kafka\Exception\Protocol
     */
    abstract public function encode(array $payloads = []): string;

    /**
     * @throws \Kafka\Exception\Protocol
     */
    abstract public function decode(string $data): array;
}
