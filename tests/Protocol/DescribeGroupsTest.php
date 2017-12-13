<?php
namespace KafkaTest\Protocol;

class DescribeGroupsTest extends \PHPUnit\Framework\TestCase
{

    /**
     * describe object
     *
     * @var mixed
     * @access protected
     */
    protected $describe = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->describe = new \Kafka\Protocol\DescribeGroups('0.9.0.1');
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

        $test = $this->describe->encode($data);
        $this->assertSame(\bin2hex($test), '0000001d000f00000000000f00096b61666b612d70687000000001000474657374');
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

        $test = $this->describe->encode($data);
        $this->assertSame(\bin2hex($test), '0000001d000f00000000000f00096b61666b612d70687000000001000474657374');
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

        $test = $this->describe->encode($data);
        $this->assertSame(\bin2hex($test), '00000017000f00000000000f00096b61666b612d70687000000000');
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $data   = '0000000100000004746573740006537461626c650008636f6e73756d6572000567726f757000000001002e6b61666b612d7068702d34646133393366622d333763662d343263632d393064642d37626636626133316664333000096b61666b612d706870000a2f3132372e302e302e31000000100000000000010004746573740000000000000018000000000001000474657374000000010000000000000000';
        $test   = $this->describe->decode(\hex2bin($data));
        $result = '[{"errorCode":0,"groupId":"test","state":"Stable","protocolType":"consumer","protocol":"group","members":[{"memberId":"kafka-php-4da393fb-37cf-42cc-90dd-7bf6ba31fd30","clientId":"kafka-php","clientHost":"\/127.0.0.1","metadata":{"version":0,"topics":["test"],"userData":""},"assignment":{"version":0,"partitions":[{"topicName":"test","partitions":[0]}],"userData":""}}]}]';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
