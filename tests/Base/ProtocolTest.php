<?php
declare(strict_types=1);

namespace KafkaTest\Base;

use Kafka\Protocol;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

class ProtocolTest extends TestCase
{
    public function testEncode(): void
    {
        Protocol::init('0.9.0.1');

        $data = [
            'group_id' => 'test',
            'member_id' => 'kafka-php-0e7cbd33-7950-40af-b691-eceaa665d297',
            'generation_id' => 2,
        ];

        $expected = '0000004d000c00000000000c00096b61666b612d70687000047465737400000002002e6b61666b612d7068702d30653763626433332d373935302d343061662d623639312d656365616136363564323937';

        self::assertSame($expected, bin2hex(Protocol::encode(Protocol::HEART_BEAT_REQUEST, $data)));
    }

    /**
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Not support api key, key:999
     */
    public function testEncodeNoKey(): void
    {
        Protocol::init('0.9.0.1');

        $data = [
            'group_id' => 'test',
            'member_id' => 'kafka-php-0e7cbd33-7950-40af-b691-eceaa665d297',
            'generation_id' => 2,
        ];

        Protocol::encode(999, $data);
    }

    /**
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Not support api key, key:999
     */
    public function testDecodeNoKey(): void
    {
        Protocol::init('0.9.0.1');
        $data = '';

        Protocol::decode(999, $data);
    }

    public function testDecode(): void
    {
        Protocol::init('0.9.0.1');

        $test = Protocol::decode(Protocol::HEART_BEAT_REQUEST, hex2bin('0000'));

        self::assertJsonStringEqualsJsonString('{"errorCode":0}', json_encode($test));
    }

    /**
     * @test
     *
     * @dataProvider errorCodesAndExpectedMessages
     */
    public function errorMessageShouldBeCorrectlyGenerated(int $errorCode, string $message): void
    {
        self::assertSame($message, Protocol::getError($errorCode));
    }

    /**
     * @return int&string[]
     */
    public function errorCodesAndExpectedMessages(): array
    {
        return [
            [0, 'No error--it worked!'],
            [-1, 'An unexpected server error'],
            [1, 'The requested offset is outside the range of offsets maintained by the server for the given topic/partition.'],
            [2, 'This indicates that a message contents does not match its CRC'],
            [3, 'This request is for a topic or partition that does not exist on this broker.'],
            [4, 'The message has a negative size'],
            [5, 'This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes'],
            [6, 'This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.'],
            [7, 'This error is thrown if the request exceeds the user-specified time limit in the request.'],
            [8, 'This is not a client facing error and is used only internally by intra-cluster broker communication.'],
            [9, 'The replica is not available for the requested topic-partition'],
            [10, 'The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.'],
            [11, 'Internal error code for broker-to-broker communication.'],
            [12, 'If you specify a string larger than configured maximum for offset metadata'],
            [13, 'The server disconnected before a response was received.'],
            [14, 'The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).'],
            [15, 'The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.'],
            [16, 'The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.'],
            [17, 'The request attempted to perform an operation on an invalid topic.'],
            [18, 'The request included message batch larger than the configured segment size on the server.'],
            [19, 'Messages are rejected since there are fewer in-sync replicas than required.'],
            [20, 'Messages are written to the log, but to fewer in-sync replicas than required.'],
            [21, 'Produce request specified an invalid value for required acks.'],
            [22, 'Specified group generation id is not valid.'],
            [23, 'The group member\'s supported protocols are incompatible with those of existing members.'],
            [24, 'The configured groupId is invalid'],
            [25, 'The coordinator is not aware of this member.'],
            [26, 'The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).'],
            [27, 'The group is rebalancing, so a rejoin is needed.'],
            [28, 'The committing offset data size is not valid'],
            [29, 'Topic authorization failed.'],
            [30, 'Group authorization failed.'],
            [31, 'Cluster authorization failed.'],
            [32, 'The timestamp of the message is out of acceptable range.'],
            [33, 'The broker does not support the requested SASL mechanism.'],
            [34, 'Request is not valid given the current SASL state.'],
            [35, 'The version of API is not supported.'],
            [36, 'Topic with this name already exists.'],
            [37, 'Number of partitions is invalid.'],
            [38, 'Replication-factor is invalid.'],
            [39, 'Replica assignment is invalid.'],
            [40, 'Configuration is invalid.'],
            [41, 'This is not the correct controller for this cluster.'],
            [42, 'This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.'],
            [43, 'The message format version on the broker does not support the request.'],
            [44, 'Request parameters do not satisfy the configured policy.'],
            [45, 'The broker received an out of order sequence number'],
            [46, 'The broker received a duplicate sequence number'],
            [47, 'Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer\'s transaction has been expired by the broker.'],
            [48, 'The producer attempted a transactional operation in an invalid state'],
            [49, 'The producer attempted to use a producer id which is not currently assigned to its transactional id'],
            [50, 'The transaction timeout is larger than the maximum value allowed by the broker (as configured by max.transaction.timeout.ms).'],
            [51, 'The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing'],
            [52, 'Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer'],
            [53, 'Transactional Id authorization failed'],
            [54, 'Security features are disabled.'],
            [55, 'The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.'],
            [56, 'Disk error when trying to access log file on the disk.'],
            [57, 'The user-specified log directory is not found in the broker config.'],
            [58, 'SASL Authentication failed.'],
            [59, 'This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question'],
            [60, 'A partition reassignment is in progress'],
            [999, 'Unknown error (999)'],
        ];
    }
}
