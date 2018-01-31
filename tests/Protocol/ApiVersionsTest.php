<?php
declare(strict_types=1);

namespace KafkaTest\Protocol;

use Kafka\Protocol\ApiVersions;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class ApiVersionsTest extends TestCase
{
    /**
     * @var ApiVersions
     */
    private $apiVersion;

    public function setUp(): void
    {
        $this->apiVersion = new ApiVersions('0.10.0.0');
    }

    public function testEncode(): void
    {
        $test = $this->apiVersion->encode();

        self::assertSame('00000013001200000000001200096b61666b612d706870', bin2hex($test));
    }

    public function testDecode(): void
    {
        $data     = '000000000026000000000005000100000006000200000002000300000005000400000001000500000000000600000004000700000001000800000003000900000003000a00000001000b00000002000c00000001000d00000001000e00000001000f00000001001000000001001100000001001200000001001300000002001400000001001500000000001600000000001700000000001800000000001900000000001a00000000001b00000000001c00000000001d00000000001e00000000001f00000000002000000000002100000000002200000000002300000000002400000000002500000000';
        $expected = '{"apiVersions":[[0,0,5],[1,0,6],[2,0,2],[3,0,5],[4,0,1],[5,0,0],[6,0,4],[7,0,1],[8,0,3],[9,0,3],[10,0,1],[11,0,2],[12,0,1],[13,0,1],[14,0,1],[15,0,1],[16,0,1],[17,0,1],[18,0,1],[19,0,2],[20,0,1],[21,0,0],[22,0,0],[23,0,0],[24,0,0],[25,0,0],[26,0,0],[27,0,0],[28,0,0],[29,0,0],[30,0,0],[31,0,0],[32,0,0],[33,0,0],[34,0,0],[35,0,0],[36,0,0],[37,0,0]],"errorCode":0}';

        $test = $this->apiVersion->decode(hex2bin($data));
        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
