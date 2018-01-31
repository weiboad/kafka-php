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

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given offset data invalid. `data` is undefined.
     */
    public function testEncodeNoData(): void
    {
        $this->offset->encode();
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given offset data invalid. `topic_name` is undefined.
     */
    public function testEncodeNoTopicName(): void
    {
        $data = [
            'data' => [
                [],
            ],
        ];

        $this->offset->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given offset data invalid. `partitions` is undefined.
     */
    public function testEncodeNoPartitions(): void
    {
        $data = [
            'data' => [
                ['topic_name' => 'test'],
            ],
        ];

        $this->offset->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given offset data invalid. `partition_id` is undefined.
     */
    public function testEncodeNoPartitionId(): void
    {
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
