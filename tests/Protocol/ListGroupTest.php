<?php
namespace KafkaTest\Protocol;

class ListGroupTest extends \PHPUnit\Framework\TestCase
{

    /**
     * list object
     *
     * @var mixed
     * @access protected
     */
    protected $list = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->list = new \Kafka\Protocol\ListGroup('0.9.0.1');
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
        ];

        $test = $this->list->encode($data);
        $this->assertSame(\bin2hex($test), '00000013001000000000001000096b61666b612d706870');
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $test   = $this->list->decode(\hex2bin('0000000000010004746573740008636f6e73756d6572'));
        $result = '{"errorCode":0,"groups":[{"groupId":"test","protocolType":"consumer"}]}';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
