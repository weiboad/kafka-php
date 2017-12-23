<?php
namespace KafkaTest\Protocol;

use Kafka\Protocol\Fetch;

final class FetchTest extends \PHPUnit\Framework\TestCase
{
    private $fetch;

    public function setUp(): void
    {
        $this->fetch = new Fetch('0.9.0.1');
    }

    public function testEncode(): void
    {
        $data = [
            'max_wait_time' => 1000,
            'replica_id' => -1,
            'min_bytes' => 1000,
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                            'offset' => 45,
                            'max_bytes' => 128,
                        ]
                    ],
                ],
            ],
        ];

        $expected = '0000003d000100010000000100096b61666b612d706870ffffffff000003e8000003e8000000010004746573740000000100000000000000000000002d00000080';
        $test     = $this->fetch->encode($data);

        self::assertSame($expected, \bin2hex($test));
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch kafka data invalid. `data` is undefined.
     */
    public function testEncodeNoData(): void
    {
        $this->fetch->encode();
    }

    public function testEncodeNoOffset(): void
    {
        $data = [
            'max_wait_time' => 1000,
            'replica_id' => -1,
            'min_bytes' => 1000,
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                        ]
                    ],
                ],
            ],
        ];

        $expected = '0000003d000100010000000100096b61666b612d706870ffffffff000003e8000003e8000000010004746573740000000100000000000000000000000000200000';
        $test     = $this->fetch->encode($data);

        self::assertSame($expected, \bin2hex($test));
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch data invalid. `topic_name` is undefined.
     */
    public function testEncodeNoTopicName(): void
    {
        $data = [
            'data' => [
                [],
            ],
        ];

        $this->fetch->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch data invalid. `partitions` is undefined.
     */
    public function testEncodeNoPartitions(): void
    {
        $data = [
            'data' => [
                [
                    'topic_name' => 'test',
                ],
            ],
        ];

        $this->fetch->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch data invalid. `partition_id` is undefined.
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

        $this->fetch->encode($data);
    }

    public function testDecode(): void
    {
        $data     = '00000000000000010004746573740000000100000000000000000000000007ff00000080000000000000002d0000001ccbf3a35d010000000007746573746b657900000007746573742e2e2e000000000000002e00000015bbbf9beb01000000000000000007746573742e2e2e000000000000002f0000001ccbf3a35d010000000007746573746b657900000007746573742e2e2e000000000000003000000015bbbf9b';
        $expected = '{"throttleTime":0,"topics":[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"highwaterMarkOffset":2047,"messageSetSize":128,"messages":[{"offset":45,"size":28,"message":{"crc":3421741917,"magic":1,"attr":0,"timestamp":0,"key":"testkey","value":"test..."}},{"offset":46,"size":21,"message":{"crc":3149896683,"magic":1,"attr":0,"timestamp":0,"key":"","value":"test..."}},{"offset":47,"size":28,"message":{"crc":3421741917,"magic":1,"attr":0,"timestamp":0,"key":"testkey","value":"test..."}}]}]}]}';

        $test = $this->fetch->decode(\hex2bin($data));

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
