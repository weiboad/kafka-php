<?php
namespace KafkaTest\Protocol;

class LeaveGroupTest extends \PHPUnit\Framework\TestCase
{

    /**
     * leave object
     *
     * @var mixed
     * @access protected
     */
    protected $leave = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->leave = new \Kafka\Protocol\LeaveGroup('0.9.0.1');
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
            'member_id' => 'kafka-php-eb19c0ea-4b3e-4ed0-bada-c873951c8eea'
        ];

        $test = $this->leave->encode($data);
        $this->assertSame(\bin2hex($test), '00000049000d00000000000d00096b61666b612d706870000474657374002e6b61666b612d7068702d65623139633065612d346233652d346564302d626164612d633837333935316338656561');
    }

    /**
     * testEncodeNoGroupId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given leave group data invalid. `group_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGroupId()
    {
        $data = [];

        $test = $this->leave->encode($data);
    }

    /**
     * testEncodeNoMemberId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given leave group data invalid. `member_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoMemberId()
    {
        $data = [
            'group_id' => 'test',
        ];

        $test = $this->leave->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $test   = $this->leave->decode(\hex2bin('0000'));
        $result = '{"errorCode":0}';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
