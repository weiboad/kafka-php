<?php

namespace KafkaTest\Protocol;

use Kafka\Protocol\ListGroup;

final class ListGroupTest extends \PHPUnit\Framework\TestCase
{
    private $list;

    public function setUp(): void
    {
        $this->list = new ListGroup('0.9.0.1');
    }

    public function testEncode(): void
    {
        $test = $this->list->encode();

        self::assertSame('00000013001000000000001000096b61666b612d706870', \bin2hex($test));
    }

    public function testDecode(): void
    {
        $test     = $this->list->decode(\hex2bin('0000000000010004746573740008636f6e73756d6572'));
        $expected = '{"errorCode":0,"groups":[{"groupId":"test","protocolType":"consumer"}]}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
