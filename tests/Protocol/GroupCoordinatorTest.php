<?php
namespace KafkaTest\Protocol;

class GroupCoordinatorTest extends \PHPUnit\Framework\TestCase
{

    /**
     * group object
     *
     * @var mixed
     * @access protected
     */
    protected $group = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->group = new \Kafka\Protocol\GroupCoordinator('0.9.0.1');
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
        ];

        $test = $this->group->encode($data);
        $this->assertSame(\bin2hex($test), '00000019000a00000000000a00096b61666b612d706870000474657374');
    }

    /**
     * testEncodeNoGroupId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given group coordinator invalid. `group_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGroupId()
    {
        $data = [
        ];

        $test = $this->group->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $data   = '000000000003000b31302e31332e342e313539000023e8';
        $test   = $this->group->decode(\hex2bin($data));
        $result = '{"errorCode":0,"coordinatorId":3,"coordinatorHost":"10.13.4.159","coordinatorPort":9192}';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
