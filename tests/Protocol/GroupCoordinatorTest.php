<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\GroupCoordinator;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class GroupCoordinatorTest extends TestCase
{
    /**
     * @var GroupCoordinator
     */
    private $group;

    public function setUp(): void
    {
        $this->group = new GroupCoordinator('0.9.0.1');
    }

    public function testEncode(): void
    {
        $data = ['group_id' => 'test'];

        $test = $this->group->encode($data);
        self::assertSame('00000019000a00000000000a00096b61666b612d706870000474657374', bin2hex($test));
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given group coordinator invalid. `group_id` is undefined.
     */
    public function testEncodeNoGroupId(): void
    {
        $this->group->encode();
    }

    public function testDecode(): void
    {
        $data     = '000000000003000b31302e31332e342e313539000023e8';
        $expected = '{"errorCode":0,"coordinatorId":3,"coordinatorHost":"10.13.4.159","coordinatorPort":9192}';

        $test = $this->group->decode(hex2bin($data));

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
