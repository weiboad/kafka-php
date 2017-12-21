<?php
namespace KafkaTest\Protocol;

use Psr\Log\NullLogger;

class MetadataTest extends \PHPUnit\Framework\TestCase
{

    /**
     * meta object
     *
     * @var mixed
     * @access protected
     */
    protected $meta = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->meta = new \Kafka\Protocol\Metadata('0.9.0.1');
        $this->meta->setLogger(new NullLogger());
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
            'test'
        ];

        $test = $this->meta->encode($data);
        $this->assertSame(\bin2hex($test), '0000001d000300000000000300096b61666b612d70687000000001000474657374');
    }

    /**
     * testEncodeString
     *
     * @access public
     * @return void
     */
    public function testEncodeString()
    {
        $data = 'test';

        $test = $this->meta->encode($data);
        $this->assertSame(\bin2hex($test), '0000001d000300000000000300096b61666b612d70687000000001000474657374');
    }

    /**
     * testEncodeEmptyArray
     *
     * @access public
     * @return void
     */
    public function testEncodeEmptyArray()
    {
        $data = [
        ];

        $test = $this->meta->encode($data);
        $this->assertSame(\bin2hex($test), '00000017000300000000000300096b61666b612d70687000000000');
    }

    /**
     * testEncodeValidTopic
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage request metadata topic array have invalid value.
     * @access public
     * @return void
     */
    public function testEncodeValidTopic()
    {
        $data = [
            1
        ];

        $test = $this->meta->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $data   = '0000000500000009000c31302e37372e39362e313337000023e800000007000b31302e37352e32362e3234000023e800000003000b31302e31332e342e313539000023e800000008000c31302e37372e39362e313336000023e800000004000b31302e31332e342e313630000023e80000000100000004746573740000000a000000000008000000030000000300000003000000070000000800000003000000030000000800000007000000000002000000090000000300000009000000030000000400000003000000040000000900000003000000000005000000070000000300000007000000090000000300000003000000030000000900000007000000000004000000040000000300000004000000070000000800000003000000040000000800000007000000000007000000090000000300000009000000040000000700000003000000040000000900000007000000000001000000080000000300000008000000090000000300000003000000090000000800000003000000000009000000040000000300000004000000080000000900000003000000040000000900000008000000000003000000030000000300000003000000040000000700000003000000030000000400000007000000000006000000080000000300000008000000030000000400000003000000040000000300000008000000000000000000070000000300000007000000080000000900000003000000080000000900000007';
        $test   = $this->meta->decode(\hex2bin($data));
        $result = '{"brokers":[{"host":"10.77.96.137","port":9192,"nodeId":9},{"host":"10.75.26.24","port":9192,"nodeId":7},{"host":"10.13.4.159","port":9192,"nodeId":3},{"host":"10.77.96.136","port":9192,"nodeId":8},{"host":"10.13.4.160","port":9192,"nodeId":4}],"topics":[{"topicName":"test","errorCode":0,"partitions":[{"partitionId":8,"errorCode":0,"replicas":[3,7,8],"leader":3,"isr":[3,8,7]},{"partitionId":2,"errorCode":0,"replicas":[9,3,4],"leader":9,"isr":[4,9,3]},{"partitionId":5,"errorCode":0,"replicas":[7,9,3],"leader":7,"isr":[3,9,7]},{"partitionId":4,"errorCode":0,"replicas":[4,7,8],"leader":4,"isr":[4,8,7]},{"partitionId":7,"errorCode":0,"replicas":[9,4,7],"leader":9,"isr":[4,9,7]},{"partitionId":1,"errorCode":0,"replicas":[8,9,3],"leader":8,"isr":[9,8,3]},{"partitionId":9,"errorCode":0,"replicas":[4,8,9],"leader":4,"isr":[4,9,8]},{"partitionId":3,"errorCode":0,"replicas":[3,4,7],"leader":3,"isr":[3,4,7]},{"partitionId":6,"errorCode":0,"replicas":[8,3,4],"leader":8,"isr":[4,3,8]},{"partitionId":0,"errorCode":0,"replicas":[7,8,9],"leader":7,"isr":[8,9,7]}]}]}';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
