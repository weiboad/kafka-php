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
* Kafka protocol for container
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Protocol
{
    // {{{ consts

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

    // protocol error code
    const NO_ERROR = 0;
    const ERROR_UNKNOWN = -1;
    const OFFSET_OUT_OF_RANGE = 1;
    const INVALID_MESSAGE = 2;
    const UNKNOWN_TOPIC_OR_PARTITION = 3;
    const INVALID_MESSAGE_SIZE = 4;
    const LEADER_NOT_AVAILABLE = 5;
    const NOT_LEADER_FOR_PARTITION = 6;
    const REQUEST_TIMED_OUT = 7;
    const BROKER_NOT_AVAILABLE = 8;
    const REPLICA_NOT_AVAILABLE = 9;
    const MESSAGE_SIZE_TOO_LARGE = 10;
    const STALE_CONTROLLER_EPOCH = 11;
    const OFFSET_METADATA_TOO_LARGE = 12;
    const GROUP_LOAD_IN_PROGRESS = 14;
    const GROUP_COORDINATOR_NOT_AVAILABLE = 15;
    const NOT_COORDINATOR_FOR_GROUP = 16;
    const INVALID_TOPIC = 17;
    const RECORD_LIST_TOO_LARGE = 18;
    const NOT_ENOUGH_REPLICAS = 19;
    const NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20;
    const INVALID_REQUIRED_ACKS = 21;
    const ILLEGAL_GENERATION = 22;
    const INCONSISTENT_GROUP_PROTOCOL = 23;
    const INVALID_GROUP_ID = 24;
    const UNKNOWN_MEMBER_ID = 25;
    const INVALID_SESSION_TIMEOUT = 26;
    const REBALANCE_IN_PROGRESS = 27;
    const INVALID_COMMIT_OFFSET_SIZE = 28;
    const TOPIC_AUTHORIZATION_FAILED = 29;
    const GROUP_AUTHORIZATION_FAILED = 30;
    const CLUSTER_AUTHORIZATION_FAILED = 31;
    const UNSUPPORTED_FOR_MESSAGE_FORMAT = 43;

    // }}}
    // {{{ members

    protected static $objects = array();

    // }}}
    // {{{ functions
    // {{{ public static function init()
    
    public static function init($version, $logger = null)
    {
        $class = array(
            \Kafka\Protocol\Protocol::PRODUCE_REQUEST => 'Produce',
            \Kafka\Protocol\Protocol::FETCH_REQUEST => 'Fetch',
            \Kafka\Protocol\Protocol::OFFSET_REQUEST => 'Offset',
            \Kafka\Protocol\Protocol::METADATA_REQUEST => 'Metadata',
            \Kafka\Protocol\Protocol::OFFSET_COMMIT_REQUEST => 'CommitOffset',
            \Kafka\Protocol\Protocol::OFFSET_FETCH_REQUEST => 'FetchOffset',
            \Kafka\Protocol\Protocol::GROUP_COORDINATOR_REQUEST => 'GroupCoordinator',
            \Kafka\Protocol\Protocol::JOIN_GROUP_REQUEST => 'JoinGroup',
            \Kafka\Protocol\Protocol::HEART_BEAT_REQUEST => 'Heartbeat',
            \Kafka\Protocol\Protocol::LEAVE_GROUP_REQUEST => 'LeaveGroup',
            \Kafka\Protocol\Protocol::SYNC_GROUP_REQUEST => 'SyncGroup',
            \Kafka\Protocol\Protocol::DESCRIBE_GROUPS_REQUEST => 'DescribeGroups',
            \Kafka\Protocol\Protocol::LIST_GROUPS_REQUEST => 'ListGroup',
        );

        $namespace = '\\Kafka\\Protocol\\';
        foreach ($class as $key => $className) {
            $class = $namespace . $className;
            self::$objects[$key] = new $class($version);
            if ($logger) {
                self::$objects[$key]->setLogger($logger);
            }
        }
    }

    // }}}
    // {{{ public static function encode()

    /**
     * request encode
     *
     * @param key $appkey
     * @param array $payloads
     * @access public
     * @return string
     */
    public static function encode($key, $payloads)
    {
        if (!isset(self::$objects[$key])) {
            throw new \Kafka\Exception('Not support api key, key:' . $key);
        }

        return self::$objects[$key]->encode($payloads);
    }

    // }}}
    // {{{ public static function decode()

    /**
     * decode response
     *
     * @access public
     * @return array
     */
    public static function decode($key, $data)
    {
        if (!isset(self::$objects[$key])) {
            throw new \Kafka\Exception('Not support api key, key:' . $key);
        }

        return self::$objects[$key]->decode($data);
    }

    // }}}
    // {{{ public static function getError()

    /**
     * get error
     *
     * @param integer $errCode
     * @static
     * @access public
     * @return string
     */
    public static function getError($errCode)
    {
        switch ($errCode) {
            case 0:
                $error = 'No error--it worked!';
                break;
            case -1:
                $error = 'An unexpected server error';
                break;
            case 1:
                $error = 'The requested offset is outside the range of offsets maintained by the server for the given topic/partition.';
                break;
            case 2:
                $error = 'This indicates that a message contents does not match its CRC';
                break;
            case 3:
                $error = 'This request is for a topic or partition that does not exist on this broker.';
                break;
            case 4:
                $error = 'The message has a negative size';
                break;
            case 5:
                $error = 'This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes';
                break;
            case 6:
                $error = 'This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.';
                break;
            case 7:
                $error = 'This error is thrown if the request exceeds the user-specified time limit in the request.';
                break;
            case 8:
                $error = 'This is not a client facing error and is used only internally by intra-cluster broker communication.';
                break;
            case 9:
                $error = 'The replica is not available for the requested topic-partition';
                break;
            case 10:
                $error = 'The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.';
                break;
            case 11:
                $error = 'Internal error code for broker-to-broker communication.';
                break;
            case 12:
                $error = 'If you specify a string larger than configured maximum for offset metadata';
                break;
            case 13:
                $error = 'The server disconnected before a response was received.';
                break;
            case 14:
                $error = 'The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).';
                break;
            case 15:
                $error = 'The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.';
                break;
            case 16:
                $error = 'The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.';
                break;
            case 17:
                $error = 'The request attempted to perform an operation on an invalid topic.';
                break;
            case 18:
                $error = 'The request included message batch larger than the configured segment size on the server.';
                break;
            case 19:
                $error = 'Messages are rejected since there are fewer in-sync replicas than required.';
                break;
            case 20:
                $error = 'Messages are written to the log, but to fewer in-sync replicas than required.';
                break;
            case 21:
                $error = 'Produce request specified an invalid value for required acks.';
                break;
            case 22:
                $error = 'Specified group generation id is not valid.';
                break;
            case 23:
                $error = 'The group member\'s supported protocols are incompatible with those of existing members.';
                break;
            case 24:
                $error = 'The configured groupId is invalid';
                break;
            case 25:
                $error = 'The coordinator is not aware of this member.';
                break;
            case 26:
                $error = 'The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).';
                break;
            case 27:
                $error = 'The group is rebalancing, so a rejoin is needed.';
                break;
            case 28:
                $error = 'The committing offset data size is not valid';
                break;
            case 29:
                $error = 'Topic authorization failed.';
                break;
            case 30:
                $error = 'Group authorization failed.';
                break;
            case 31:
                $error = 'Cluster authorization failed.';
                break;
            case 32:
                $error = 'The timestamp of the message is out of acceptable range.';
                break;
            case 33:
                $error = 'The broker does not support the requested SASL mechanism.';
                break;
            case 34:
                $error = 'Request is not valid given the current SASL state.';
                break;
            case 35:
                $error = 'The version of API is not supported.';
                break;
            default:
                $error = 'Unknown error';
        }

        return $error;
    }

    // }}}
    // }}}
}
