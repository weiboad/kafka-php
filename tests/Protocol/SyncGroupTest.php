<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\SyncGroup;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_decode;
use function json_encode;

final class SyncGroupTest extends TestCase
{
    /**
     * @var SyncGroup
     */
    private $sync;

    public function setUp(): void
    {
        $this->sync = new SyncGroup('0.9.0.1');
    }

    public function testEncode(): void
    {
        $data = json_decode(
            '{"group_id":"test","generation_id":1,"member_id":"kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c","data":[{"version":0,"member_id":"kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c","assignments":[{"topic_name":"test","partitions":[0]}]}]}',
            true
        );

        $expected = '0000009d000e00000000000e00096b61666b612d70687000047465737400000001002e6b61666b612d7068702d62643564356262322d326131662d343364342d623833312d62313531306438316163356300000001002e6b61666b612d7068702d62643564356262322d326131662d343364342d623833312d62313531306438316163356300000018000000000001000474657374000000010000000000000000';

        self::assertSame($expected, bin2hex($this->sync->encode($data)));
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given sync group data invalid. `group_id` is undefined.
     */
    public function testEncodeNoGroupId(): void
    {
        $this->sync->encode();
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given sync group data invalid. `generation_id` is undefined.
     */
    public function testEncodeNoGenerationId(): void
    {
        $data = ['group_id' => 'test'];

        $this->sync->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given sync group data invalid. `member_id` is undefined.
     */
    public function testEncodeNoMemberId(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
        ];

        $this->sync->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given sync group data invalid. `data` is undefined.
     */
    public function testEncodeNoData(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
        ];

        $this->sync->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `version` is undefined.
     */
    public function testEncodeNoVersion(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => [
                [],
            ],
        ];

        $this->sync->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `member_id` is undefined.
     */
    public function testEncodeNoDataMemberId(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => [
                ['version' => 0],
            ],
        ];

        $this->sync->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `assignments` is undefined.
     */
    public function testEncodeNoDataAssignments(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => [
                [
                    'version' => 0 ,
                    'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
                ],
            ],
        ];

        $this->sync->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `topic_name` is undefined.
     */
    public function testEncodeNoTopicName(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => [
                [
                    'version' => 0 ,
                    'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
                    'assignments' => [
                        [],
                    ],
                ],
            ],
        ];

        $this->sync->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `partitions` is undefined.
     */
    public function testEncodeNoPartitions(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => [
                [
                    'version' => 0 ,
                    'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
                    'assignments' => [
                        ['topic_name' => 'test'],
                    ],
                ],
            ],
        ];

        $this->sync->encode($data);
    }

    public function testDecode(): void
    {
        $data     = '000000000018000000000001000474657374000000010000000000000000';
        $expected = '{"errorCode":0,"partitionAssignments":[{"topicName":"test","partitions":[0]}],"version":0,"userData":""}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($this->sync->decode(hex2bin($data))));
        self::assertJsonStringEqualsJsonString('{"errorCode":0}', json_encode($this->sync->decode(hex2bin('000000000000'))));
    }
}
