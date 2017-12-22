<?php
namespace KafkaTest\Protocol;

class SyncGroupTest extends \PHPUnit\Framework\TestCase
{

    /**
     * sync object
     *
     * @var mixed
     * @access protected
     */
    protected $sync = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->sync = new \Kafka\Protocol\SyncGroup('0.9.0.1');
    }

    /**
     * testEncode
     *
     * @access public
     * @return void
     */
    public function testEncode()
    {
        $data = json_decode('{"group_id":"test","generation_id":1,"member_id":"kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c","data":[{"version":0,"member_id":"kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c","assignments":[{"topic_name":"test","partitions":[0]}]}]}', true);

        $test = $this->sync->encode($data);
        $this->assertSame(\bin2hex($test), '0000009d000e00000000000e00096b61666b612d70687000047465737400000001002e6b61666b612d7068702d62643564356262322d326131662d343364342d623833312d62313531306438316163356300000001002e6b61666b612d7068702d62643564356262322d326131662d343364342d623833312d62313531306438316163356300000018000000000001000474657374000000010000000000000000');
    }

    /**
     * testEncodeNoGroupId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given sync group data invalid. `group_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGroupId()
    {
        $data = [];

        $test = $this->sync->encode($data);
    }

    /**
     * testEncodeNoGenerationId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given sync group data invalid. `generation_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGenerationId()
    {
        $data = [
            'group_id' => 'test',
        ];

        $test = $this->sync->encode($data);
    }

    /**
     * testEncodeNoMemberId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given sync group data invalid. `member_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoMemberId()
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
        ];

        $test = $this->sync->encode($data);
    }

    /**
     * testEncodeNoData
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given sync group data invalid. `data` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoData()
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c'
        ];

        $test = $this->sync->encode($data);
    }

    /**
     * testEncodeNoVersion
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `version` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoVersion()
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => [
                [

                ],
            ],
        ];

        $test = $this->sync->encode($data);
    }

    /**
     * testEncodeNoDataMemberId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `member_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoDataMemberId()
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => [
                [
                    'version' => 0
                ],
            ],
        ];

        $test = $this->sync->encode($data);
    }

    /**
     * testEncodeNoDataAssignments
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `assignments` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoDataAssignments()
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

        $test = $this->sync->encode($data);
    }

    /**
     * testEncodeNoTopicName
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `topic_name` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoTopicName()
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
                    ]
                ],
            ],
        ];

        $test = $this->sync->encode($data);
    }

    /**
     * testEncodeNoPartitions
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `partitions` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoPartitions()
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
                        [
                            'topic_name' => 'test',
                        ],
                    ]
                ],
            ],
        ];

        $test = $this->sync->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $data   = '000000000018000000000001000474657374000000010000000000000000';
        $test   = $this->sync->decode(\hex2bin($data));
        $result = '{"errorCode":0,"partitionAssignments":[{"topicName":"test","partitions":[0]}],"version":0,"userData":""}';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
        $test   = $this->sync->decode(\hex2bin('000000000000'));
        $result = '{"errorCode":0}';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
