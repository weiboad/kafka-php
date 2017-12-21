<?php
namespace KafkaTest\Protocol;

use Psr\Log\NullLogger;

class HeartbeatTest extends \PHPUnit\Framework\TestCase
{

    /**
     * heart object
     *
     * @var mixed
     * @access protected
     */
    protected $heart = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->heart = new \Kafka\Protocol\Heartbeat('0.9.0.1');
        $this->heart->setLogger(new NullLogger());
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
            'member_id' => 'kafka-php-0e7cbd33-7950-40af-b691-eceaa665d297',
            'generation_id' => 2,
        ];
        $test = $this->heart->encode($data);
        $this->assertSame(\bin2hex($test), '0000004d000c00000000000c00096b61666b612d70687000047465737400000002002e6b61666b612d7068702d30653763626433332d373935302d343061662d623639312d656365616136363564323937');
    }

    /**
     * testEncodeNoGroupId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given heartbeat data invalid. `group_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGroupId()
    {
        $data = [];

        $test = $this->heart->encode($data);
    }

    /**
     * testEncodeNoGenerationId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given heartbeat data invalid. `generation_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGenerationId()
    {
        $data = [
            'group_id' => 'test',
        ];

        $test = $this->heart->encode($data);
    }

    /**
     * testEncodeNoMemberId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given heartbeat data invalid. `member_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoMemberId()
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
        ];

        $test = $this->heart->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $test   = $this->heart->decode(\hex2bin('0000'));
        $result = '{"errorCode":0}';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
