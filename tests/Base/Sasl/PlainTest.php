<?php
namespace KafkaTest\Base\Sasl;

use \Kafka\Sasl\Plain;
use \Kafka\Config\Sasl;

class PlainTest extends \PHPUnit\Framework\TestCase
{

    private $plain;

    public function setUp()
    {
        $config = new Sasl();
        $config->setUsername('nmred');
        $config->setPassword('123456');
        $this->plain = new Plain($config);
    }

    /**
     * testHandShake
     *
     * @access public
     * @return void
     */
    public function testHandShake()
    {
        // Create a stub for the SomeClass class.
        $socket = $this->createMock(\Kafka\Socket\SocketBlocking::class);

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

        $this->plain->authenticate($socket);
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
        $socket = $this->createMock(\Kafka\Socket\SocketBlocking::class);

        $handShakeData = \hex2bin('00000011002100000004000d534352414d2d5348412d3531320005504c41494e0006475353415049000d534352414d2d5348412d323536');
        // Configure the stub.
        $socket->method('readBlocking')
            ->will($this->onConsecutiveCalls(\hex2bin('00000037'), $handShakeData));
        $socket->expects($this->exactly(1))
            ->method('writeBlocking')
            ->withConsecutive(
                [$this->equalTo(\hex2bin('0000001a001100000000001100096b61666b612d7068700005504c41494e'))]
            );

        $this->plain->authenticate($socket);
    }

    /**
     * testGetMechanismName
     *
     * @access public
     * @return void
     */
    public function testGetMechanismName()
    {
        $this->assertSame('PLAIN', $this->plain->getName());
    }
}
