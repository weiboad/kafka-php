<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\ListGroup;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class ListGroupTest extends TestCase
{
    /**
     * @var ListGroup
     */
    private $list;

    public function setUp(): void
    {
        $this->list = new ListGroup('0.9.0.1');
    }

    public function testEncode(): void
    {
        $test = $this->list->encode();

        self::assertSame('00000013001000000000001000096b61666b612d706870', bin2hex($test));
    }

    public function testDecode(): void
    {
        $test     = $this->list->decode(hex2bin('0000000000010004746573740008636f6e73756d6572'));
        $expected = '{"errorCode":0,"groups":[{"groupId":"test","protocolType":"consumer"}]}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
