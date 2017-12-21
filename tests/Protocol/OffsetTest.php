<?php
namespace KafkaTest\Protocol;

use Psr\Log\NullLogger;

class OffsetTest extends \PHPUnit\Framework\TestCase
{

    /**
     * offset object
     *
     * @var mixed
     * @access protected
     */
    protected $offset = null;

    /**
     * offset object v0.10
     *
     * @var mixed
     * @access protected
     */
    protected $offset10 = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $nullLogger   = new NullLogger();
        $this->offset = new \Kafka\Protocol\Offset('0.9.0.1');
        $this->offset->setLogger($nullLogger);
        $this->offset10 = new \Kafka\Protocol\Offset('0.10.1.0');
        $this->offset10->setLogger($nullLogger);
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
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                            'offset' => 100,
                        ],
                    ],
                ],
            ],
        ];

        $test = $this->offset->encode($data);
        $this->assertSame(\bin2hex($test), '00000035000200000000000200096b61666b612d706870ffffffff000000010004746573740000000100000000ffffffffffffffff000186a0');
        $test = $this->offset10->encode($data);
        $this->assertSame(\bin2hex($test), '00000035000200000000000200096b61666b612d706870ffffffff000000010004746573740000000100000000ffffffffffffffff000186a0');
    }

    /**
     * testEncodeNoData
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given offset data invalid. `data` is undefined.
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
     * testEncodeNoTopicName
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given offset data invalid. `topic_name` is undefined.
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

        $test = $this->offset->encode($data);
    }

    /**
     * testEncodeNoPartitions
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given offset data invalid. `partitions` is undefined.
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

        $test = $this->offset->encode($data);
    }

    /**
     * testEncodeNoPartitionId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given offset data invalid. `partition_id` is undefined.
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
        $data   = '000000010004746573740000000100000000000000000001000000000000002a';
        $test   = $this->offset->decode(\hex2bin($data));
        $result = '[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"timestamp":0,"offsets":[42]}]}]';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
