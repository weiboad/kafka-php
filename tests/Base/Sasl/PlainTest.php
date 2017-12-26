<?php
namespace KafkaTest\Base\Sasl;

use PHPUnit\Framework\TestCase;
use Kafka\Socket;
use Kafka\Sasl\Plain;

class PlainTest extends TestCase
{

    /**
     * testHandShake
     *
     * @access public
     * @return void
     */
    public function testHandShake()
    {
        // Create a stub for the SomeClass class.
        $socket = $this->createMock(Socket::class);

        $handShakeData = \hex2bin('00000011000000000004000d534352414d2d5348412d3531320005504c41494e0006475353415049000d534352414d2d5348412d323536');
        // Configure the stub.
        $socket->method('readBlocking')
            ->will($this->onConsecutiveCalls(\hex2bin('00000037'), $handShakeData, \hex2bin('00000000')));
        $socket->expects($this->exactly(2))
            ->method('writeBlocking')
            ->withConsecutive(
                [$this->equalTo(\hex2bin('0000001a001100000000001100096b61666b612d7068700005504c41494e'))],
                [$this->equalTo(\hex2bin('0000000d006e6d72656400313233343536'))]
            );

        $provider = new Plain('nmred', '123456');
        $provider->authenticate($socket);
    }

    /**
     * testHandShake
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage The broker does not support the requested SASL mechanism.
     * @access public
     * @return void
     */
    public function testHandShakeNotSupport()
    {
        // Create a stub for the SomeClass class.
        $socket = $this->createMock(\Kafka\Socket::class);

        $handShakeData = \hex2bin('00000011002100000004000d534352414d2d5348412d3531320005504c41494e0006475353415049000d534352414d2d5348412d323536');
        // Configure the stub.
        $socket->method('readBlocking')
            ->will($this->onConsecutiveCalls(\hex2bin('00000037'), $handShakeData));
        $socket->expects($this->exactly(1))
            ->method('writeBlocking')
            ->withConsecutive(
                [$this->equalTo(\hex2bin('0000001a001100000000001100096b61666b612d7068700005504c41494e'))]
            );

        $provider = new \Kafka\Sasl\Plain('nmred', '123456');
        $provider->authenticate($socket);
    }

    /**
     * testGetMechanismName
     *
     * @access public
     * @return void
     */
    public function testGetMechanismName()
    {
        $provider = new \Kafka\Sasl\Plain('nmred', '123456');
        $this->assertSame('PLAIN', $provider->getName());
    }
}
