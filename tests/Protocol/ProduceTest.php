<?php
namespace KafkaTest\Protocol;

class ProduceTest extends \PHPUnit\Framework\TestCase
{

    /**
     * produce object
     *
     * @var mixed
     * @access protected
     */
    protected $produce = null;

    /**
     * produce object v0.10.1.0
     *
     * @var mixed
     * @access protected
     */
    protected $produce10 = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->produce   = new \Kafka\Protocol\Produce('0.9.0.1');
        $this->produce10 = new \Kafka\Protocol\Produce('0.10.1.0');
    }

    /**
     * testEncode
     *
     * @access public
     * @return void
     */
    public function testEncode()
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

        $test = $this->produce->encode($data);
        $this->assertSame(\bin2hex($test), '00000092000000010000000000096b61666b612d7068700001000003e800000001000474657374000000010000000000000063000000000000000000000015bbbf9beb01000000000000000007746573742e2e2e000000000000000100000015bbbf9beb01000000000000000007746573742e2e2e000000000000000200000015bbbf9beb01000000000000000007746573742e2e2e');
    }

    /**
     * testEncodeForMessageKey
     *
     * @access public
     * @return void
     */
    public function testEncodeForMessageKey()
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
                                ['key' => 'testkey', 'value' => 'test...']
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $test = $this->produce10->encode($data);
        $this->assertSame(\bin2hex($test), '00000057000000020000000000096b61666b612d7068700001000003e80000000100047465737400000001000000000000002800000000000000000000001c4ad6c67a000000000007746573746b657900000007746573742e2e2e');
    }

    /**
     * testEncodeForMessage
     *
     * @access public
     * @return void
     */
    public function testEncodeForMessage()
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
                            'messages' => 'test...'
                        ],
                    ],
                ],
            ],
        ];

        $test = $this->produce10->encode($data);
        $this->assertSame(\bin2hex($test), '00000050000000020000000000096b61666b612d7068700001000003e8000000010004746573740000000100000000000000210000000000000000000000153c1950a800000000000000000007746573742e2e2e');
    }

    /**
     * testEncodeNotTimeoutAndRequired
     *
     * @access public
     * @return void
     */
    public function testEncodeNotTimeoutAndRequired()
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

        $test = $this->produce->encode($data);
        $this->assertSame(\bin2hex($test), '00000092000000010000000000096b61666b612d70687000000000006400000001000474657374000000010000000000000063000000000000000000000015bbbf9beb01000000000000000007746573742e2e2e000000000000000100000015bbbf9beb01000000000000000007746573742e2e2e000000000000000200000015bbbf9beb01000000000000000007746573742e2e2e');
    }

    /**
     * testEncodeNoData
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given procude data invalid. `data` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoData()
    {
        $data = [
        ];

        $test = $this->produce->encode($data);
    }

    /**
     * testEncodeNoTopicName
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given produce data invalid. `topic_name` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoTopicName()
    {
        $data = [
            'data' => [
                [
                ],
            ],
        ];

        $test = $this->produce->encode($data);
    }

    /**
     * testEncodeNoPartitions
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given produce data invalid. `partitions` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoPartitions()
    {
        $data = [
            'data' => [
                [
                    'topic_name' => 'test',
                ],
            ],
        ];

        $test = $this->produce->encode($data);
    }

    /**
     * testEncodeNoPartitionId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given produce data invalid. `partition_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoPartitionId()
    {
        $data = [
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                        ],
                    ],
                ],
            ],
        ];

        $test = $this->produce->encode($data);
    }

    /**
     * testEncodeNoMessage
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given produce data invalid. `messages` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoMessage()
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
                        ],
                    ],
                ],
            ],
        ];

        $test = $this->produce->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $data   = '0000000100047465737400000001000000000000000000000000002a00000000';
        $test   = $this->produce->decode(\hex2bin($data));
        $result = '{"throttleTime":0,"data":[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"offset":14,"timestamp":0}]}]}';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
        $data   = '0000000100047465737400000001000000000000000000000000006effffffffffffffff00000000';
        $test   = $this->produce10->decode(\hex2bin($data));
        $result = '{"throttleTime":0,"data":[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"offset":22,"timestamp":-1}]}]}';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
