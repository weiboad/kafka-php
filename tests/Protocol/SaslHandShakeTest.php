<?php
namespace KafkaTest\Protocol;

use Psr\Log\NullLogger;

class SaslHandShakeTest extends \PHPUnit\Framework\TestCase
{

    /**
     * sasl object
     *
     * @var mixed
     * @access protected
     */
    protected $sasl = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->sasl = new \Kafka\Protocol\SaslHandShake('0.10.0.0');
        $this->sasl->setLogger(new NullLogger());
    }

    /**
     * testEncode
     *
     * @access public
     * @return void
     */
    public function testEncode()
    {
        $data = 'PLAIN';
        $test = $this->sasl->encode($data);
        $this->assertSame(\bin2hex($test), '0000001a001100000000001100096b61666b612d7068700005504c41494e');
    }

    /**
     * testEncodeIsNotString
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage Invalid request SASL hand shake mechanism given.
     * @access public
     * @return void
     */
    public function testEncodeIsNotString()
    {
        $data = [
        ];

        $test = $this->sasl->encode($data);
    }

    /**
     * testEncodeIsNotAllow
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessageRegExp /Invalid request SASL hand shake mechanism given, it must be one of: \w+/
     * @access public
     * @return void
     */
    public function testEncodeIsNotAllow()
    {
        $data = 'NOTALLOW';

        $test = $this->sasl->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $data   = '0022000000010006475353415049';
        $test   = $this->sasl->decode(\hex2bin($data));
        $result = '{"mechanisms":["GSSAPI"],"errorCode":34}';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
