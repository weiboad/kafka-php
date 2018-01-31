<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\Produce;
use Lcobucci\Clock\FrozenClock;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class ProduceTest extends TestCase
{
    /**
     * @var Produce
     */
    private $produce;

    /**
     * @var Produce
     */
    private $produce10;

    public function setUp(): void
    {
        $clock = new FrozenClock(new \DateTimeImmutable('2017-12-24 11:12:10.123'));

        $this->produce   = new Produce('0.9.0.1', $clock);
        $this->produce10 = new Produce('0.10.1.0', $clock);
    }

    public function testEncode(): void
    {
        $data = [
            'required_ack' => 1,
            'timeout' => '1000',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                            'messages' => [
                                'test...',
                                'test...',
                                'test...',
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $expected = '00000092000000010000000000096b61666b612d7068700001000003e8000000010004746573740000000100000000000000630000000000000000000000153c1950a800000000000000000007746573742e2e2e0000000000000001000000153c1950a800000000000000000007746573742e2e2e0000000000000002000000153c1950a800000000000000000007746573742e2e2e';

        self::assertSame($expected, bin2hex($this->produce->encode($data)));
    }

    public function testEncodeForMessageKey(): void
    {
        $data = [
            'required_ack' => 1,
            'timeout' => '1000',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                            'messages' => [
                                ['key' => 'testkey', 'value' => 'test...'],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $expected = '0000005f000000020000000000096b61666b612d7068700001000003e80000000100047465737400000001000000000000003000000000000000000000002422d04d210100000001608839138b00000007746573746b657900000007746573742e2e2e';

        self::assertSame($expected, bin2hex($this->produce10->encode($data)));
    }

    public function testEncodeForMessage(): void
    {
        $data = [
            'required_ack' => 1,
            'timeout' => '1000',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                            'messages' => 'test...',
                        ],
                    ],
                ],
            ],
        ];

        $expected = '00000058000000020000000000096b61666b612d7068700001000003e80000000100047465737400000001000000000000002900000000000000000000001d53d771b70100000001608839138b0000000000000007746573742e2e2e';
        self::assertSame($expected, bin2hex($this->produce10->encode($data)));
    }

    public function testEncodeNotTimeoutAndRequired(): void
    {
        $data = [
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                            'messages' => [
                                'test...',
                                'test...',
                                'test...',
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $expected = '00000092000000010000000000096b61666b612d706870000000000064000000010004746573740000000100000000000000630000000000000000000000153c1950a800000000000000000007746573742e2e2e0000000000000001000000153c1950a800000000000000000007746573742e2e2e0000000000000002000000153c1950a800000000000000000007746573742e2e2e';

        self::assertSame($expected, bin2hex($this->produce->encode($data)));
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given procude data invalid. `data` is undefined.
     */
    public function testEncodeNoData(): void
    {
        $this->produce->encode();
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given produce data invalid. `topic_name` is undefined.
     */
    public function testEncodeNoTopicName(): void
    {
        $data = [
            'data' => [
                [],
            ],
        ];

        $this->produce->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given produce data invalid. `partitions` is undefined.
     */
    public function testEncodeNoPartitions(): void
    {
        $data = [
            'data' => [
                ['topic_name' => 'test'],
            ],
        ];

        $this->produce->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given produce data invalid. `partition_id` is undefined.
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

        $this->produce->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given produce data invalid. `messages` is undefined.
     */
    public function testEncodeNoMessage(): void
    {
        $data = [
            'required_ack' => 1,
            'timeout' => '1000',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        ['partition_id' => 0],
                    ],
                ],
            ],
        ];

        $this->produce->encode($data);
    }

    public function testDecode(): void
    {
        $data     = '0000000100047465737400000001000000000000000000000000002a00000000';
        $expected = '{"throttleTime":0,"data":[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"offset":14,"timestamp":0}]}]}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($this->produce->decode(hex2bin($data))));
    }

    public function testDecodeKafka10(): void
    {
        $data     = '0000000100047465737400000001000000000000000000000000006effffffffffffffff00000000';
        $expected = '{"throttleTime":0,"data":[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"offset":22,"timestamp":-1}]}]}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($this->produce10->decode(hex2bin($data))));
    }
}
