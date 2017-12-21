<?php
namespace KafkaTest\Protocol;

use Psr\Log\NullLogger;

class JoinGroupTest extends \PHPUnit\Framework\TestCase
{

    /**
     * group object v0.9.0
     *
     * @var mixed
     * @access protected
     */
    protected $group9 = null;

    /**
     * group object v0.10.0
     *
     * @var mixed
     * @access protected
     */
    protected $group10 = null;

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $nullLogger   = new NullLogger();
        $this->group9 = new \Kafka\Protocol\JoinGroup('0.9.0.1');
        $this->group9->setLogger($nullLogger);
        $this->group10 = new \Kafka\Protocol\JoinGroup('0.10.1.0');
        $this->group10->setLogger($nullLogger);
    }

    /**
     * testEncode
     *
     * @access public
     * @return void
     */
    public function testEncode()
    {
        $data  = [
            'group_id' => 'test',
            'session_timeout' => 6000,
            'member_id' => '',
            'data' => [
                [
                    'protocol_name' => 'group',
                    'version' => 0,
                    'subscription' => ['test'],
                ],
            ],
        ];
        $test9 = $this->group9->encode($data);
        $this->assertSame(\bin2hex($test9), '00000048000b00000000000b00096b61666b612d7068700004746573740000177000000008636f6e73756d657200000001000567726f75700000001000000000000100047465737400000000');
        $test10 = $this->group10->encode($data);
        $this->assertSame(\bin2hex($test10), '0000004c000b00010000000b00096b61666b612d706870000474657374000017700000177000000008636f6e73756d657200000001000567726f75700000001000000000000100047465737400000000');
    }

    /**
     * testEncodeNoGroupId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given join group data invalid. `group_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGroupId()
    {
        $data = [
        ];

        $test = $this->group9->encode($data);
    }

    /**
     * testEncodeNoSessionTimeout
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given join group data invalid. `session_timeout` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoSessionTimeout()
    {
        $data = [
            'group_id' => 'test',
        ];

        $test = $this->group9->encode($data);
    }

    /**
     * testEncodeNoMemberId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given join group data invalid. `member_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoMemberId()
    {
        $data = [
            'group_id' => 'test',
            'session_timeout' => 6000,
        ];

        $test = $this->group9->encode($data);
    }

    /**
     * testEncodeNoData
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given join group data invalid. `data` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoData()
    {
        $data = [
            'group_id' => 'test',
            'session_timeout' => 6000,
            'member_id' => '',
        ];

        $test = $this->group9->encode($data);
    }

    /**
     * testEncodeHasProtocolType
     *
     * @access public
     * @return void
     */
    public function testEncodeHasProtocolType()
    {
        $data  = [
            'group_id' => 'test',
            'session_timeout' => 6000,
            'rebalance_timeout' => 6000,
            'member_id' => '',
            'protocol_type' => 'testtype',
            'data' => [
                [
                    'protocol_name' => 'group',
                    'version' => 0,
                    'subscription' => ['test'],
                    'user_data' => '',
                ],
            ],
        ];
        $test9 = $this->group9->encode($data);
        $this->assertSame(\bin2hex($test9), '00000048000b00000000000b00096b61666b612d7068700004746573740000177000000008746573747479706500000001000567726f75700000001000000000000100047465737400000000');
        $test10 = $this->group10->encode($data);
        $this->assertSame(\bin2hex($test10), '0000004c000b00010000000b00096b61666b612d706870000474657374000017700000177000000008746573747479706500000001000567726f75700000001000000000000100047465737400000000');
    }

    /**
     * testEncodeNoProtocolName
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given join group data invalid. `protocol_name` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoProtocolName()
    {
        $data = [
            'group_id' => 'test',
            'session_timeout' => 6000,
            'rebalance_timeout' => 6000,
            'member_id' => '',
            'data' => [
                [
                ],
            ],
        ];

        $test = $this->group9->encode($data);
    }

    /**
     * testEncodeNoVersion
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `version` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoVersion()
    {
        $data = [
            'group_id' => 'test',
            'session_timeout' => 6000,
            'rebalance_timeout' => 6000,
            'member_id' => '',
            'data' => [
                [
                    'protocol_name' => 'group',
                ],
            ],
        ];

        $test = $this->group9->encode($data);
    }

    /**
     * testEncodeNoSubscription
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `subscription` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoSubscription()
    {
        $data = [
            'group_id' => 'test',
            'session_timeout' => 6000,
            'rebalance_timeout' => 6000,
            'member_id' => '',
            'data' => [
                [
                    'protocol_name' => 'group',
                    'version' => 0,
                ],
            ],
        ];

        $test = $this->group9->encode($data);
    }

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $data   = '000000000001000567726f7570002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d396134393335613934663366002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d39613439333561393466336600000001002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d3961343933356139346633660000001000000000000100047465737400000000';
        $test   = $this->group9->decode(\hex2bin($data));
        $result = '{"errorCode":0,"generationId":1,"groupProtocol":"group","leaderId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","memberId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","members":[{"memberId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","memberMeta":{"version":0,"topics":["test"],"userData":""}}]}';
        $this->assertJsonStringEqualsJsonString(json_encode($test), $result);
    }
}
