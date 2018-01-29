<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\Fetch;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class FetchTest extends TestCase
{
    /**
     * @var Fetch
     */
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
                        ],
                    ],
                ],
            ],
        ];

        $expected = '0000003d000100010000000100096b61666b612d706870ffffffff000003e8000003e8000000010004746573740000000100000000000000000000002d00000080';
        $test     = $this->fetch->encode($data);

        self::assertSame($expected, bin2hex($test));
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
                        ['partition_id' => 0],
                    ],
                ],
            ],
        ];

        $expected = '0000003d000100010000000100096b61666b612d706870ffffffff000003e8000003e8000000010004746573740000000100000000000000000000000000200000';
        $test     = $this->fetch->encode($data);

        self::assertSame($expected, bin2hex($test));
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
                ['topic_name' => 'test'],
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

        $test = $this->fetch->decode(hex2bin($data));

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }

    public function testDecodeCompressedMessage(): void
    {
        $data     = '0000000000000001000474657374000000030000000200000000000000000005000000820000000000000004000000764f9bfc190101000001608f09f09dffffffff000000601f8b08000000000000006360800319c19722b68c400663423fe787b95051b6dce2745d0343280f242dd3f1c0b51d873a23288f09a44eace5820a0e75a6501e33485d86e99a9938d45940792c2075f55f455f615767680000f433673fc800000000000001000000000000000000030000006900000000000000020000005d2df3369e0101000001608f09f09dffffffff000000471f8b08000000000000006360800399ffcf4b0519810cc6847ece0f73a1a26cb9c5e9ba06c6501e485aa6bfb7711e0e7566501e1348dd8fae8d1c38d4990300959fec9d7800000000000000000000000000000000020000005c00000000000000010000005049f79c420101000001608f09f09dffffffff0000003a1f8b08000000000000006360800399c4e6079b18810cc6847ece0f73a1a26cb9c5e9ba0626501e485a46de680e3f0e759600085bf4ff50000000';
        $expected = [
            'throttleTime' => 0,
            'topics'       => [
                [
                    'topicName'  => 'test',
                    'partitions' => [
                        [
                            'partition'           => 2,
                            'errorCode'           => 0,
                            'highwaterMarkOffset' => 5,
                            'messageSetSize'      => 130,
                            'messages'            => [
                                [
                                    'offset'  => 0,
                                    'size'    => 28,
                                    'message' => [
                                        'crc'       => 300487741,
                                        'magic'     => 1,
                                        'attr'      => 0,
                                        'timestamp' => 1514228281501,
                                        'key'       => '',
                                        'value'     => 'msg-01',
                                    ],
                                ],
                                [
                                    'offset'  => 1,
                                    'size'    => 28,
                                    'message' => [
                                        'crc'       => 2296399239,
                                        'magic'     => 1,
                                        'attr'      => 0,
                                        'timestamp' => 1514228281501,
                                        'key'       => '',
                                        'value'     => 'msg-02',
                                    ],
                                ],
                                [
                                    'offset'  => 2,
                                    'size'    => 28,
                                    'message' => [
                                        'crc'       => 377802788,
                                        'magic'     => 1,
                                        'attr'      => 0,
                                        'timestamp' => 1514228281501,
                                        'key'       => '',
                                        'value'     => 'msg-05',
                                    ],
                                ],
                                [
                                    'offset'  => 3,
                                    'size'    => 28,
                                    'message' => [
                                        'crc'       => 1748348057,
                                        'magic'     => 1,
                                        'attr'      => 0,
                                        'timestamp' => 1514228281501,
                                        'key'       => '',
                                        'value'     => 'msg-08',
                                    ],
                                ],
                                [
                                    'offset'  => 4,
                                    'size'    => 28,
                                    'message' => [
                                        'crc'       => 2146768362,
                                        'magic'     => 1,
                                        'attr'      => 0,
                                        'timestamp' => 1514228281501,
                                        'key'       => '',
                                        'value'     => 'msg-10',
                                    ],
                                ],
                            ],
                        ],
                        [
                            'partition'           => 1,
                            'errorCode'           => 0,
                            'highwaterMarkOffset' => 3,
                            'messageSetSize'      => 105,
                            'messages'            => [
                                [
                                    'offset'  => 0,
                                    'size'    => 28,
                                    'message' => [
                                        'crc'       => 4293358865,
                                        'magic'     => 1,
                                        'attr'      => 0,
                                        'timestamp' => 1514228281501,
                                        'key'       => '',
                                        'value'     => 'msg-03',
                                    ],
                                ],
                                [
                                    'offset'  => 1,
                                    'size'    => 28,
                                    'message' => [
                                        'crc'       => 2408415646,
                                        'magic'     => 1,
                                        'attr'      => 0,
                                        'timestamp' => 1514228281501,
                                        'key'       => '',
                                        'value'     => 'msg-06',
                                    ],
                                ],
                                [
                                    'offset'  => 2,
                                    'size'    => 28,
                                    'message' => [
                                        'crc'       => 4169838856,
                                        'magic'     => 1,
                                        'attr'      => 0,
                                        'timestamp' => 1514228281501,
                                        'key'       => '',
                                        'value'     => 'msg-07',
                                    ],
                                ],
                            ],
                        ],
                        [
                            'partition'           => 0,
                            'errorCode'           => 0,
                            'highwaterMarkOffset' => 2,
                            'messageSetSize'      => 92,
                            'messages'            => [
                                [
                                    'offset'  => 0,
                                    'size'    => 28,
                                    'message' => [
                                        'crc'       => 1636032690,
                                        'magic'     => 1,
                                        'attr'      => 0,
                                        'timestamp' => 1514228281501,
                                        'key'       => '',
                                        'value'     => 'msg-04',
                                    ],
                                ],
                                [
                                    'offset'  => 1,
                                    'size'    => 28,
                                    'message' => [
                                        'crc'       => 523410447,
                                        'magic'     => 1,
                                        'attr'      => 0,
                                        'timestamp' => 1514228281501,
                                        'key'       => '',
                                        'value'     => 'msg-09',
                                    ],
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $test = $this->fetch->decode(hex2bin($data));

        self::assertEquals($expected, $test);
    }
}
