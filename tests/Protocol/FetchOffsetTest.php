<?php
namespace KafkaTest\Protocol;

class FetchOffsetTest extends \PHPUnit\Framework\TestCase
{

    /**
     * offset object
     *
     * @var mixed
     * @access protected
     */
    protected $offset = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->offset = new \Kafka\Protocol\FetchOffset('0.9.0.1');
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
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [0]
                ]
            ],
        ];

        $test = $this->offset->encode($data);
        $this->assertSame(\bin2hex($test), '0000002b000900010000000900096b61666b612d706870000474657374000000010004746573740000000100000000');
    }

    /**
     * testEncodeNoData
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch offset data invalid. `data` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoData()
    {
        $data = [
        ];

        $test = $this->offset->encode($data);
    }

    /**
     * testEncodeNoGroupId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch offset data invalid. `group_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGroupId()
    {
        $data = [
            'data' => []
        ];

        $test = $this->offset->encode($data);
    }

    /**
     * testEncodeNoTopicName
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch offset data invalid. `topic_name` is undefined.
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

        $test = $this->offset->encode($data);
    }

    /**
     * testEncodeNoPartitions
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch offset data invalid. `partitions` is undefined.
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
        $test = $this->offset->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $data   = '000000010004746573740000000100000000ffffffffffffffff00000000';
        $test   = $this->offset->decode(\hex2bin($data));
        $result = '[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"metadata":"","offset":-1}]}]';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
