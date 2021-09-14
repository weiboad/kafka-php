<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\Offset;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class OffsetTest extends TestCase
{
    /**
     * @var Offset
     */
    private $offset;

    /**
     * @var Offset
     */
    private $offset10;

    public function setUp(): void
    {
        $this->offset   = new Offset('0.9.0.1');
        $this->offset10 = new Offset('0.10.1.0');
    }

    public function testEncode(): void
    {
        $data = [
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                            'offset'       => 100,
                        ],
                    ],
                ],
            ],
        ];

        $expected = '00000035000200000000000200096b61666b612d706870ffffffff000000010004746573740000000100000000ffffffffffffffff000186a0';

        self::assertSame($expected, bin2hex($this->offset->encode($data)));
        self::assertSame($expected, bin2hex($this->offset10->encode($data)));
    }

    public function testEncodeNoData(): void
    {
        $this->expectExceptionMessage("given offset data invalid. `data` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $this->offset->encode();
    }

    public function testEncodeNoTopicName(): void
    {
        $this->expectExceptionMessage("given offset data invalid. `topic_name` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $data = [
            'data' => [
                [],
            ],
        ];

        $this->offset->encode($data);
    }

    public function testEncodeNoPartitions(): void
    {
        $this->expectExceptionMessage("given offset data invalid. `partitions` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $data = [
            'data' => [
                ['topic_name' => 'test'],
            ],
        ];

        $this->offset->encode($data);
    }

    public function testEncodeNoPartitionId(): void
    {
        $this->expectExceptionMessage("given offset data invalid. `partition_id` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $data = [
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [],
                    ],
                ],
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
        $data     = '000000010004746573740000000100000000000000000001000000000000002a';
        $expected = '[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"timestamp":0,"offsets":[42]}]}]';

        $test = $this->offset->decode(hex2bin($data));

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
