<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\SaslHandShake;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class SaslHandShakeTest extends TestCase
{
    /**
     * @var SaslHandShake
     */
    private $sasl;

    public function setUp(): void
    {
        $this->sasl = new SaslHandShake('0.10.0.0');
    }

    public function testEncode(): void
    {
        $test = $this->sasl->encode(['PLAIN']);

        self::assertSame('0000001a001100000000001100096b61666b612d7068700005504c41494e', bin2hex($test));
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage Invalid request SASL hand shake mechanism given.
     */
    public function testEncodeNoMechanismGiven(): void
    {
        $this->sasl->encode();
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessageRegExp /Invalid request SASL hand shake mechanism given, it must be one of: \w+/
     */
    public function testEncodeInvalidMechanism(): void
    {
        $this->sasl->encode(['NOTALLOW']);
    }

    /**
     * testDecode
     *
     * @access public
     */
    public function testDecode(): void
    {
        $data     = '0022000000010006475353415049';
        $expected = '{"mechanisms":["GSSAPI"],"errorCode":34}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($this->sasl->decode(hex2bin($data))));
    }
}
