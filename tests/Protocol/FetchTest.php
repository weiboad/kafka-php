<?php
namespace KafkaTest\Protocol;

class FetchTest extends \PHPUnit\Framework\TestCase
{

    /**
     * fetch object
     *
     * @var mixed
     * @access protected
     */
    protected $fetch = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->fetch = new \Kafka\Protocol\Fetch('0.9.0.1');
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

        $test = $this->fetch->encode($data);
        $this->assertSame(\bin2hex($test), '0000003d000100010000000100096b61666b612d706870ffffffff000003e8000003e8000000010004746573740000000100000000000000000000002d00000080');
    }

    /**
     * testEncodeNoData
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch kafka data invalid. `data` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoData()
    {
        $data = [
        ];

        $test = $this->fetch->encode($data);
    }

    /**
     * testEncodeNoOffset
     *
     * @access public
     * @return void
     */
    public function testEncodeNoOffset()
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

        $test = $this->fetch->encode($data);
        $this->assertSame(\bin2hex($test), '0000003d000100010000000100096b61666b612d706870ffffffff000003e8000003e8000000010004746573740000000100000000000000000000000000200000');
    }

    /**
     * testEncodeNoTopicName
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch data invalid. `topic_name` is undefined.
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

        $test = $this->fetch->encode($data);
    }

    /**
     * testEncodeNoPartitions
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch data invalid. `partitions` is undefined.
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

        $test = $this->fetch->encode($data);
    }

    /**
     * testEncodeNoPartitionId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch data invalid. `partition_id` is undefined.
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

        $test = $this->fetch->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $data   = '00000000000000010004746573740000000100000000000000000000000007ff00000080000000000000002d0000001ccbf3a35d010000000007746573746b657900000007746573742e2e2e000000000000002e00000015bbbf9beb01000000000000000007746573742e2e2e000000000000002f0000001ccbf3a35d010000000007746573746b657900000007746573742e2e2e000000000000003000000015bbbf9b';
        $test   = $this->fetch->decode(\hex2bin($data));
        $result = '{"throttleTime":0,"topics":[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"highwaterMarkOffset":2047,"messageSetSize":128,"messages":[{"offset":45,"size":28,"message":{"crc":3421741917,"magic":1,"attr":0,"timestamp":0,"key":"testkey","value":"test..."}},{"offset":46,"size":21,"message":{"crc":3149896683,"magic":1,"attr":0,"timestamp":0,"key":"","value":"test..."}},{"offset":47,"size":28,"message":{"crc":3421741917,"magic":1,"attr":0,"timestamp":0,"key":"testkey","value":"test..."}}]}]}]}';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
