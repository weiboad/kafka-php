<?php
namespace KafkaTest\Protocol;

use Psr\Log\NullLogger;

class CommitOffsetTest extends \PHPUnit\Framework\TestCase
{

    /**
     * commit object
     *
     * @var mixed
     * @access protected
     */
    protected $commit = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->commit = new \Kafka\Protocol\CommitOffset('0.9.0.1');
        $this->commit->setLogger(new NullLogger());
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
            'group_id' => 'test',
            'generation_id' => 2,
            'member_id' => 'kafka-php-c7e3d40a-57d8-4220-9523-cebfce9a0685',
            'retention_time' => 36000,
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition' => 0,
                            'offset' => 45,
                            'metadata' => '',
                        ]
                    ]
                ]
            ],
        ];
        $test = $this->commit->encode($data);
        $this->assertSame(\bin2hex($test), '00000071000800020000000800096b61666b612d70687000047465737400000002002e6b61666b612d7068702d63376533643430612d353764382d343232302d393532332d6365626663653961303638350000000000008ca0000000010004746573740000000100000000000000000000002d0000');
    }

    /**
     * testEncodeDefault
     *
     * @access public
     * @return void
     */
    public function testEncodeDefault()
    {
        $data = [
            'group_id' => 'test',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition' => 0,
                            'offset' => 45,
                        ]
                    ]
                ]
            ],
        ];
        $test = $this->commit->encode($data);
        $this->assertSame(\bin2hex($test), '00000043000800020000000800096b61666b612d706870000474657374ffffffff0000ffffffffffffffff000000010004746573740000000100000000000000000000002d0000');
    }

    /**
     * testEncodeNoData
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given commit data invalid. `data` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoData()
    {
        $data = [
            'group_id' => 'test'
        ];

        $test = $this->commit->encode($data);
    }

    /**
     * testEncodeNoGroupId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given commit offset data invalid. `group_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGroupId()
    {
        $data = [
            'data' => []
        ];

        $test = $this->commit->encode($data);
    }

    /**
     * testEncodeNoTopicName
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given commit offset data invalid. `topic_name` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoTopicName()
    {
        $data = [
            'group_id' => 'test',
            'data' => [
                [
                ]
            ],
        ];

        $test = $this->commit->encode($data);
    }

    /**
     * testEncodeNoPartitions
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given commit offset data invalid. `partitions` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoPartitions()
    {
        $data = [
            'group_id' => 'test',
            'data' => [
                [
                    'topic_name' => 'test',
                ]
            ],
        ];
        $test = $this->commit->encode($data);
    }

    /**
     * testEncodeNoPartition
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given commit offset data invalid. `partition` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoPartition()
    {
        $data = [
            'group_id' => 'test',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        []
                    ]
                ]
            ],
        ];
        $test = $this->commit->encode($data);
    }

    /**
     * testEncodeNoOffset
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given commit offset data invalid. `offset` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoOffset()
    {
        $data = [
            'group_id' => 'test',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition' => 0
                        ]
                    ]
                ]
            ],
        ];
        $test = $this->commit->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $data   = '0000000100047465737400000001000000000000';
        $test   = $this->commit->decode(\hex2bin($data));
        $result = '[{"topicName":"test","partitions":[{"partition":0,"errorCode":0}]}]';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
