<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\LeaveGroup;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class LeaveGroupTest extends TestCase
{
    /**
     * @var LeaveGroup
     */
    private $leave;

    public function setUp(): void
    {
        $this->leave = new LeaveGroup('0.9.0.1');
    }

    public function testEncode(): void
    {
        $data = [
            'group_id'  => 'test',
            'member_id' => 'kafka-php-eb19c0ea-4b3e-4ed0-bada-c873951c8eea',
        ];

        $expected = '00000049000d00000000000d00096b61666b612d706870000474657374002e6b61666b612d7068702d65623139633065612d346233652d346564302d626164612d633837333935316338656561';
        $test     = $this->leave->encode($data);

        self::assertSame($expected, bin2hex($test));
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given leave group data invalid. `group_id` is undefined.
     */
    public function testEncodeNoGroupId(): void
    {
        $this->leave->encode();
    }

    /**
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given leave group data invalid. `member_id` is undefined.
     */
    public function testEncodeNoMemberId(): void
    {
        $data = ['group_id' => 'test'];

        $this->leave->encode($data);
    }

    public function testDecode(): void
    {
        $test     = $this->leave->decode(hex2bin('0000'));
        $expected = '{"errorCode":0}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
