<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\JoinGroup;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class JoinGroupTest extends TestCase
{
    /**
     * @var JoinGroup
     */
    private $group9;

    /**
     * @var JoinGroup
     */
    private $group10;

    public function setUp(): void
    {
        $this->group9  = new JoinGroup('0.9.0.1');
        $this->group10 = new JoinGroup('0.10.1.0');
    }

    public function testEncode(): void
    {
        $data = [
            'group_id'        => 'test',
            'session_timeout' => 6000,
            'member_id'       => '',
            'data'            => [
                [
                    'protocol_name' => 'group',
                    'version'       => 0,
                    'subscription'  => ['test'],
                ],
            ],
        ];

        $expected9  = '00000048000b00000000000b00096b61666b612d7068700004746573740000177000000008636f6e73756d657200000001000567726f75700000001000000000000100047465737400000000';
        $expected10 = '0000004c000b00010000000b00096b61666b612d706870000474657374000017700000177000000008636f6e73756d657200000001000567726f75700000001000000000000100047465737400000000';
        $test9      = $this->group9->encode($data);
        $test10     = $this->group10->encode($data);

        self::assertSame($expected9, bin2hex($test9));
        self::assertSame($expected10, bin2hex($test10));
    }

    public function testEncodeNoGroupId(): void
    {
        $this->expectExceptionMessage("given join group data invalid. `group_id` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $this->group9->encode();
    }

    public function testEncodeNoSessionTimeout(): void
    {
        $this->expectExceptionMessage("given join group data invalid. `session_timeout` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $data = ['group_id' => 'test'];

        $this->group9->encode($data);
    }

    /**
     * testEncodeNoMemberId
     *
     *
     *
     */
    public function testEncodeNoMemberId(): void
    {
        $this->expectExceptionMessage("given join group data invalid. `member_id` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $data = [
            'group_id'        => 'test',
            'session_timeout' => 6000,
        ];

        $test = $this->group9->encode($data);
    }

    public function testEncodeNoData(): void
    {
        $this->expectExceptionMessage("given join group data invalid. `data` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $data = [
            'group_id'        => 'test',
            'session_timeout' => 6000,
            'member_id'       => '',
        ];

        $this->group9->encode($data);
    }

    public function testEncodeHasProtocolType(): void
    {
        $data = [
            'group_id'          => 'test',
            'session_timeout'   => 6000,
            'rebalance_timeout' => 6000,
            'member_id'         => '',
            'protocol_type'     => 'testtype',
            'data'              => [
                [
                    'protocol_name' => 'group',
                    'version'       => 0,
                    'subscription'  => ['test'],
                    'user_data'     => '',
                ],
            ],
        ];

        $expected9  = '00000048000b00000000000b00096b61666b612d7068700004746573740000177000000008746573747479706500000001000567726f75700000001000000000000100047465737400000000';
        $expected10 = '0000004c000b00010000000b00096b61666b612d706870000474657374000017700000177000000008746573747479706500000001000567726f75700000001000000000000100047465737400000000';
        $test9      = $this->group9->encode($data);
        $test10     = $this->group10->encode($data);

        self::assertSame($expected9, bin2hex($test9));
        self::assertSame($expected10, bin2hex($test10));
    }

    public function testEncodeNoProtocolName(): void
    {
        $this->expectExceptionMessage("given join group data invalid. `protocol_name` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $data = [
            'group_id'          => 'test',
            'session_timeout'   => 6000,
            'rebalance_timeout' => 6000,
            'member_id'         => '',
            'data'              => [
                [],
            ],
        ];

        $this->group9->encode($data);
    }

    public function testEncodeNoVersion(): void
    {
        $this->expectExceptionMessage("given data invalid. `version` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $data = [
            'group_id'          => 'test',
            'session_timeout'   => 6000,
            'rebalance_timeout' => 6000,
            'member_id'         => '',
            'data'              => [
                ['protocol_name' => 'group'],
            ],
        ];

        $test = $this->group9->encode($data);
    }

    public function testEncodeNoSubscription(): void
    {
        $this->expectExceptionMessage("given data invalid. `subscription` is undefined.");
        $this->expectException(\Kafka\Exception\Protocol::class);
        $data = [
            'group_id'          => 'test',
            'session_timeout'   => 6000,
            'rebalance_timeout' => 6000,
            'member_id'         => '',
            'data'              => [
                [
                    'protocol_name' => 'group',
                    'version'       => 0,
                ],
            ],
        ];

        $this->group9->encode($data);
    }

    public function testDecode(): void
    {
        $data     = '000000000001000567726f7570002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d396134393335613934663366002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d39613439333561393466336600000001002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d3961343933356139346633660000001000000000000100047465737400000000';
        $expected = '{"errorCode":0,"generationId":1,"groupProtocol":"group","leaderId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","memberId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","members":[{"memberId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","memberMeta":{"version":0,"topics":["test"],"userData":""}}]}';

        $test = $this->group9->decode(hex2bin($data));

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
