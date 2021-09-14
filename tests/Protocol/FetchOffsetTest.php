<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\FetchOffset;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class FetchOffsetTest extends TestCase
{
    /**
     * @var FetchOffset
     */
    private $offset;

    public function setUp(): void
    {
        $this->offset = new FetchOffset('0.9.0.1');
    }

    public function testEncode(): void
    {
        $data = [
            'group_id' => 'test',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [0],
                ],
            ],
        ];

        $expected = '0000002b000900010000000900096b61666b612d706870000474657374000000010004746573740000000100000000';
        $test     = $this->offset->encode($data);

        self::assertSame($expected, bin2hex($test));
    }

    public function testEncodeNoData(): void
    {
        $this->expectExceptionMessage("given fetch offset data invalid. `data` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $this->offset->encode();
    }

    public function testEncodeNoGroupId(): void
    {
        $this->expectExceptionMessage("given fetch offset data invalid. `group_id` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $data = [
            'data' => [],
        ];

        $this->offset->encode($data);
    }

    public function testEncodeNoTopicName(): void
    {
        $this->expectExceptionMessage("given fetch offset data invalid. `topic_name` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $data = [
            'group_id' => 'test',
            'data' => [
                [],
            ],
        ];

        $this->offset->encode($data);
    }

    public function testEncodeNoPartitions(): void
    {
        $this->expectExceptionMessage("given fetch offset data invalid. `partitions` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $data = [
            'group_id' => 'test',
            'data' => [
                ['topic_name' => 'test'],
            ],
        ];

        $this->offset->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     */
    public function testDecode(): void
    {
        $data     = '000000010004746573740000000100000000ffffffffffffffff00000000';
        $expected = '[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"metadata":"","offset":-1}]}]';

        $test = $this->offset->decode(hex2bin($data));
        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
