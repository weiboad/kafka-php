<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\Heartbeat;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class HeartbeatTest extends TestCase
{
    /**
     * @var Heartbeat
     */
    private $heart;

    public function setUp(): void
    {
        $this->heart = new Heartbeat('0.9.0.1');
    }

    public function testEncode(): void
    {
        $data = [
            'group_id'      => 'test',
            'member_id'     => 'kafka-php-0e7cbd33-7950-40af-b691-eceaa665d297',
            'generation_id' => 2,
        ];

        $expected = '0000004d000c00000000000c00096b61666b612d70687000047465737400000002002e6b61666b612d7068702d30653763626433332d373935302d343061662d623639312d656365616136363564323937';
        $test     = $this->heart->encode($data);

        self::assertSame($expected, bin2hex($test));
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given heartbeat data invalid. `group_id` is undefined.
     */
    public function testEncodeNoGroupId(): void
    {
        $this->heart->encode();
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given heartbeat data invalid. `generation_id` is undefined.
     */
    public function testEncodeNoGenerationId(): void
    {
        $data = ['group_id' => 'test'];

        $this->heart->encode($data);
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given heartbeat data invalid. `member_id` is undefined.
     */
    public function testEncodeNoMemberId(): void
    {
        $data = [
            'group_id'      => 'test',
            'generation_id' => '1',
        ];

        $this->heart->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     */
    public function testDecode(): void
    {
        $test     = $this->heart->decode(hex2bin('0000'));
        $expected = '{"errorCode":0}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
