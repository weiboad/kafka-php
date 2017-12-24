<?php
namespace Kafka;

use Psr\Log\LoggerInterface;

class Protocol
{
    /**
     * protocol request code
     */
    public const PRODUCE_REQUEST           = 0;
    public const FETCH_REQUEST             = 1;
    public const OFFSET_REQUEST            = 2;
    public const METADATA_REQUEST          = 3;
    public const OFFSET_COMMIT_REQUEST     = 8;
    public const OFFSET_FETCH_REQUEST      = 9;
    public const GROUP_COORDINATOR_REQUEST = 10;
    public const JOIN_GROUP_REQUEST        = 11;
    public const HEART_BEAT_REQUEST        = 12;
    public const LEAVE_GROUP_REQUEST       = 13;
    public const SYNC_GROUP_REQUEST        = 14;
    public const DESCRIBE_GROUPS_REQUEST   = 15;
    public const LIST_GROUPS_REQUEST       = 16;
    public const SASL_HAND_SHAKE_REQUEST   = 17;
    public const API_VERSIONS_REQUEST      = 18;
    public const CREATE_TOPICS_REQUEST     = 19;
    public const DELETE_TOPICS_REQUEST     = 20;

    // protocol error code
    public const NO_ERROR                              = 0;
    public const ERROR_UNKNOWN                         = -1;
    public const OFFSET_OUT_OF_RANGE                   = 1;
    public const INVALID_MESSAGE                       = 2;
    public const UNKNOWN_TOPIC_OR_PARTITION            = 3;
    public const INVALID_MESSAGE_SIZE                  = 4;
    public const LEADER_NOT_AVAILABLE                  = 5;
    public const NOT_LEADER_FOR_PARTITION              = 6;
    public const REQUEST_TIMED_OUT                     = 7;
    public const BROKER_NOT_AVAILABLE                  = 8;
    public const REPLICA_NOT_AVAILABLE                 = 9;
    public const MESSAGE_SIZE_TOO_LARGE                = 10;
    public const STALE_CONTROLLER_EPOCH                = 11;
    public const OFFSET_METADATA_TOO_LARGE             = 12;
    public const GROUP_LOAD_IN_PROGRESS                = 14;
    public const GROUP_COORDINATOR_NOT_AVAILABLE       = 15;
    public const NOT_COORDINATOR_FOR_GROUP             = 16;
    public const INVALID_TOPIC                         = 17;
    public const RECORD_LIST_TOO_LARGE                 = 18;
    public const NOT_ENOUGH_REPLICAS                   = 19;
    public const NOT_ENOUGH_REPLICAS_AFTER_APPEND      = 20;
    public const INVALID_REQUIRED_ACKS                 = 21;
    public const ILLEGAL_GENERATION                    = 22;
    public const INCONSISTENT_GROUP_PROTOCOL           = 23;
    public const INVALID_GROUP_ID                      = 24;
    public const UNKNOWN_MEMBER_ID                     = 25;
    public const INVALID_SESSION_TIMEOUT               = 26;
    public const REBALANCE_IN_PROGRESS                 = 27;
    public const INVALID_COMMIT_OFFSET_SIZE            = 28;
    public const TOPIC_AUTHORIZATION_FAILED            = 29;
    public const GROUP_AUTHORIZATION_FAILED            = 30;
    public const CLUSTER_AUTHORIZATION_FAILED          = 31;
    public const INVALID_TIMESTAMP                     = 32;
    public const UNSUPPORTED_SASL_MECHANISM            = 33;
    public const ILLEGAL_SASL_STATE                    = 34;
    public const UNSUPPORTED_VERSION                   = 35;
    public const TOPIC_ALREADY_EXISTS                  = 36;
    public const INVALID_PARTITIONS                    = 37;
    public const INVALID_REPLICATION_FACTOR            = 38;
    public const INVALID_REPLICA_ASSIGNMENT            = 39;
    public const INVALID_CONFIG                        = 40;
    public const NOT_CONTROLLER                        = 41;
    public const INVALID_REQUEST                       = 42;
    public const UNSUPPORTED_FOR_MESSAGE_FORMAT        = 43;
    public const POLICY_VIOLATION                      = 44;
    public const OUT_OF_ORDER_SEQUENCE_NUMBER          = 45;
    public const DUPLICATE_SEQUENCE_NUMBER             = 46;
    public const INVALID_PRODUCER_EPOCH                = 47;
    public const INVALID_TXN_STATE                     = 48;
    public const INVALID_PRODUCER_ID_MAPPING           = 49;
    public const INVALID_TRANSACTION_TIMEOUT           = 50;
    public const CONCURRENT_TRANSACTIONS               = 51;
    public const TRANSACTION_COORDINATOR_FENCED        = 52;
    public const TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53;
    public const SECURITY_DISABLED                     = 54;
    public const OPERATION_NOT_ATTEMPTED               = 55;
    public const KAFKA_STORAGE_ERROR                   = 56;
    public const LOG_DIR_NOT_FOUND                     = 57;
    public const SASL_AUTHENTICATION_FAILED            = 58;
    public const UNKNOWN_PRODUCER_ID                   = 59;
    public const REASSIGNMENT_IN_PROGRESS              = 60;

    private const PROTOCOL_ERROR_MAP = [
        0  => 'No error--it worked!',
        -1 => 'An unexpected server error',
        1  => 'The requested offset is outside the range of offsets maintained by the server for the given topic/partition.',
        2  => 'This indicates that a message contents does not match its CRC',
        3  => 'This request is for a topic or partition that does not exist on this broker.',
        4  => 'The message has a negative size',
        5  => 'This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes',
        6  => 'This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.',
        7  => 'This error is thrown if the request exceeds the user-specified time limit in the request.',
        8  => 'This is not a client facing error and is used only internally by intra-cluster broker communication.',
        9  => 'The replica is not available for the requested topic-partition',
        10 => 'The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.',
        11 => 'Internal error code for broker-to-broker communication.',
        12 => 'If you specify a string larger than configured maximum for offset metadata',
        13 => 'The server disconnected before a response was received.',
        14 => 'The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).',
        15 => 'The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.',
        16 => 'The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.',
        17 => 'The request attempted to perform an operation on an invalid topic.',
        18 => 'The request included message batch larger than the configured segment size on the server.',
        19 => 'Messages are rejected since there are fewer in-sync replicas than required.',
        20 => 'Messages are written to the log, but to fewer in-sync replicas than required.',
        21 => 'Produce request specified an invalid value for required acks.',
        22 => 'Specified group generation id is not valid.',
        23 => 'The group member\'s supported protocols are incompatible with those of existing members.',
        24 => 'The configured groupId is invalid',
        25 => 'The coordinator is not aware of this member.',
        26 => 'The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).',
        27 => 'The group is rebalancing, so a rejoin is needed.',
        28 => 'The committing offset data size is not valid',
        29 => 'Topic authorization failed.',
        30 => 'Group authorization failed.',
        31 => 'Cluster authorization failed.',
        32 => 'The timestamp of the message is out of acceptable range.',
        33 => 'The broker does not support the requested SASL mechanism.',
        34 => 'Request is not valid given the current SASL state.',
        35 => 'The version of API is not supported.',
        36 => 'Topic with this name already exists.',
        37 => 'Number of partitions is invalid.',
        38 => 'Replication-factor is invalid.',
        39 => 'Replica assignment is invalid.',
        40 => 'Configuration is invalid.',
        41 => 'This is not the correct controller for this cluster.',
        42 => 'This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.',
        43 => 'The message format version on the broker does not support the request.',
        44 => 'Request parameters do not satisfy the configured policy.',
        45 => 'The broker received an out of order sequence number',
        46 => 'The broker received a duplicate sequence number',
        47 => 'Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer\'s transaction has been expired by the broker.',
        48 => 'The producer attempted a transactional operation in an invalid state',
        49 => 'The producer attempted to use a producer id which is not currently assigned to its transactional id',
        50 => 'The transaction timeout is larger than the maximum value allowed by the broker (as configured by max.transaction.timeout.ms).',
        51 => 'The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing',
        52 => 'Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer',
        53 => 'Transactional Id authorization failed',
        54 => 'Security features are disabled.',
        55 => 'The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.',
        56 => 'Disk error when trying to access log file on the disk.',
        57 => 'The user-specified log directory is not found in the broker config.',
        58 => 'SASL Authentication failed.',
        59 => 'This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question',
        60 => 'A partition reassignment is in progress'
    ];

    /**
     * @var Protocol\Protocol[]
     */
    protected static $objects = [];

    public static function init(string $version, LoggerInterface $logger = null): void
    {
        $class = [
            Protocol\Protocol::PRODUCE_REQUEST           => Protocol\Produce::class,
            Protocol\Protocol::FETCH_REQUEST             => Protocol\Fetch::class,
            Protocol\Protocol::OFFSET_REQUEST            => Protocol\Offset::class,
            Protocol\Protocol::METADATA_REQUEST          => Protocol\Metadata::class,
            Protocol\Protocol::OFFSET_COMMIT_REQUEST     => Protocol\CommitOffset::class,
            Protocol\Protocol::OFFSET_FETCH_REQUEST      => Protocol\FetchOffset::class,
            Protocol\Protocol::GROUP_COORDINATOR_REQUEST => Protocol\GroupCoordinator::class,
            Protocol\Protocol::JOIN_GROUP_REQUEST        => Protocol\JoinGroup::class,
            Protocol\Protocol::HEART_BEAT_REQUEST        => Protocol\Heartbeat::class,
            Protocol\Protocol::LEAVE_GROUP_REQUEST       => Protocol\LeaveGroup::class,
            Protocol\Protocol::SYNC_GROUP_REQUEST        => Protocol\SyncGroup::class,
            Protocol\Protocol::DESCRIBE_GROUPS_REQUEST   => Protocol\DescribeGroups::class,
            Protocol\Protocol::LIST_GROUPS_REQUEST       => Protocol\ListGroup::class,
            Protocol\Protocol::SASL_HAND_SHAKE_REQUEST   => Protocol\SaslHandShake::class,
            Protocol\Protocol::API_VERSIONS_REQUEST      => Protocol\ApiVersions::class,
        ];

        foreach ($class as $key => $className) {
            self::$objects[$key] = new $className($version);

            if ($logger) {
                self::$objects[$key]->setLogger($logger);
            }
        }
    }

    /**
     * @throws \Kafka\Exception
     */
    public static function encode(int $key, array $payloads): string
    {
        if (! isset(self::$objects[$key])) {
            throw new \Kafka\Exception('Not support api key, key:' . $key);
        }

        return self::$objects[$key]->encode($payloads);
    }

    /**
     * @throws \Kafka\Exception
     */
    public static function decode(int $key, string $data): array
    {
        if (! isset(self::$objects[$key])) {
            throw new \Kafka\Exception('Not support api key, key:' . $key);
        }

        return self::$objects[$key]->decode($data);
    }

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
        if (! isset(self::PROTOCOL_ERROR_MAP[$errCode])) {
            return 'Unknown error (' . $errCode . ')';
        }

        return self::PROTOCOL_ERROR_MAP[$errCode];
    }
}
