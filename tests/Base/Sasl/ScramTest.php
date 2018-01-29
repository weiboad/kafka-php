<?php
declare(strict_types=1);

namespace KafkaTest\Base\Sasl;

use Kafka\Sasl\Scram;
use Kafka\Socket;
use PHPUnit\Framework\TestCase;
use function hex2bin;

class ScramTest extends TestCase
{
    /**
     * testScram
     *
     * @access public
     */
    public function testScram(): void
    {
        $provider = $this->getMockBuilder(Scram::class)
            ->setMethods(['generateNonce'])
            ->setConstructorArgs(['alice', 'alice-secret', Scram::SCRAM_SHA_256])
            ->getMock();
        $provider->method('generateNonce')
            ->will($this->returnValue('5Fr49BaTHKn0i9ytDBMw8YXNMOemtxbJ+opDL/miWK8='));
        $socket = $this->getSocketForVerify();
        $provider->authenticate($socket);
    }

    /**
     * testScramVerify
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Verify server final response message is failure
     * @access public
     */
    public function testScramVerify(): void
    {
        $provider = $this->getMockBuilder(Scram::class)
            ->setMethods(['generateNonce'])
            ->setConstructorArgs(['alice', 'alice-secret', Scram::SCRAM_SHA_256])
            ->getMock();
        $provider->method('generateNonce')
            ->will($this->returnValue('5Fr49BaTHKn0i9ytDBMw8YXNMOemtxbJ+opDL/miWK8='));
        $socket = $this->getSocketForVerify('invalid message');
        $provider->authenticate($socket);
    }

    /**
     * testFinalMessageInvalid
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Server response challenge is invalid.
     * @access public
     */
    public function testFinalMessageInvalid(): void
    {
        $this->finalMessage();
    }

    /**
     * testFinalMessageInvalidSalt
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Server response challenge is invalid, paser salt is failure.
     * @access public
     */
    public function testFinalMessageInvalidSalt(): void
    {
        $message = 'r=5Fr49BaTHKn0i9ytDBMw8YXNMOemtxbJ+opDL/miWK8=ou7tesfefbqo5ymk9dajioxiv,s=,i=8192';
        $this->finalMessage($message);
    }

    /**
     * testFinalMessageInvalidCnonce
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Server response challenge is invalid, cnonce is invalid.
     * @access public
     */
    public function testFinalMessageInvalidCnonce(): void
    {
        $message = 'r=59BaTHKn0i9ytDBMw8YXNMOemtxbJ+opDL/miWK8=ou7tesfefbqo5ymk9dajioxiv,s=a3Vqa3JvOGRldzVpbWNxY3QwMXdzZW0yYg==,i=8192';
        $this->finalMessage($message);
    }

    /**
     * testInvalidAlgorithm
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Invalid hash algorithm given, it must be one of: [SCRAM_SHA_256, SCRAM_SHA_512].
     * @access public
     */
    public function testInvalidAlgorithm(): void
    {
        new Scram('nmred', '123456', 64);
    }

    /**
     * testGetMechanismName
     *
     * @access public
     */
    public function testGetMechanismName(): void
    {
        $provider = new Scram('nmred', '123456', Scram::SCRAM_SHA_256);
        $this->assertSame('SCRAM-SHA-256', $provider->getName());
    }

    private function getSocketForVerify(string $verifyMessage = ''): Socket
    {
        $socket             = $this->createMock(Socket::class);
        $handShakeData      = hex2bin('00000011000000000004000d534352414d2d5348412d3531320005504c41494e0006475353415049000d534352414d2d5348412d323536');
        $firstServerMessage = 'r=5Fr49BaTHKn0i9ytDBMw8YXNMOemtxbJ+opDL/miWK8=ou7tesfefbqo5ymk9dajioxiv,s=a3Vqa3JvOGRldzVpbWNxY3QwMXdzZW0yYg==,i=8192';
        $verifyMessage      = ($verifyMessage === '') ? 'v=AM496N+dPKeXeORuChQslmlCo+QHI8wy7CxRWOIMXdY=' : $verifyMessage;
        $socket->method('readBlocking')
            ->will($this->onConsecutiveCalls(
                // hand shake response data length
                hex2bin('00000037'),
                $handShakeData,
                hex2bin('00000075'), // first server message length
                $firstServerMessage,
                hex2bin('0000002e'), // final server response length
                $verifyMessage
            ));

        // first message:  n,,n=alice,r=5Fr49BaTHKn0i9ytDBMw8YXNMOemtxbJ+opDL/miWK8=
        $firstMessage = hex2bin('000000396e2c2c6e3d616c6963652c723d3546723439426154484b6e306939797444424d773859584e4d4f656d7478624a2b6f70444c2f6d69574b383d');
        // final message: c=biws,r=5Fr49BaTHKn0i9ytDBMw8YXNMOemtxbJ+opDL/miWK8=ou7tesfefbqo5ymk9dajioxiv,p=Ky7+xuihooYxDciXZYCr1j54tmc0y/ZvPN8So/1hi/w=
        $finalMessage = hex2bin('0000007d633d626977732c723d3546723439426154484b6e306939797444424d773859584e4d4f656d7478624a2b6f70444c2f6d69574b383d6f753774657366656662716f35796d6b3964616a696f7869762c703d4b79372b787569686f6f5978446369585a594372316a3534746d6330792f5a76504e38536f2f3168692f773d');

        $socket->expects($this->exactly(3))
            ->method('writeBlocking')
            ->withConsecutive(
                // write handshake request
                [$this->equalTo(hex2bin('00000022001100000000001100096b61666b612d706870000d534352414d2d5348412d323536'))],
                [$this->equalTo($firstMessage)],
                [$this->equalTo($finalMessage)]
            );
        return $socket;
    }

    private function getSocketForInvalidFinalMessage(string $serverMessage = ''): Socket
    {
        $socket        = $this->createMock(Socket::class);
        $handShakeData = hex2bin('00000011000000000004000d534352414d2d5348412d3531320005504c41494e0006475353415049000d534352414d2d5348412d323536');

        if ($serverMessage === '') {
            $firstServerMessage = 'r=5Fr49BaTHKn0i9ytDBMw8YXNMOemtxbJ+opDL/miWK8=ou7tesfefbqo5ymk9dajioxiv,s=a3Vqa3JvOGRldzVpbWNxY3QwMXdzZW0yYg';
        } else {
            $firstServerMessage = $serverMessage;
        }
        $socket->method('readBlocking')
            ->will($this->onConsecutiveCalls(
                // hand shake response data length
                hex2bin('00000037'),
                $handShakeData,
                hex2bin('00000075'), // first server message length
                $firstServerMessage
            ));

        // first message:  n,,n=alice,r=5Fr49BaTHKn0i9ytDBMw8YXNMOemtxbJ+opDL/miWK8=
        $firstMessage = hex2bin('000000396e2c2c6e3d616c6963652c723d3546723439426154484b6e306939797444424d773859584e4d4f656d7478624a2b6f70444c2f6d69574b383d');
        $socket->expects($this->exactly(2))
            ->method('writeBlocking')
            ->withConsecutive(
                // write handshake request
                [$this->equalTo(hex2bin('00000022001100000000001100096b61666b612d706870000d534352414d2d5348412d323536'))],
                [$this->equalTo($firstMessage)]
            );
        return $socket;
    }

    private function finalMessage(string $message = ''): void
    {
        $provider = $this->getMockBuilder(Scram::class)
            ->setMethods(['generateNonce', 'verifyMessage'])
            ->setConstructorArgs(['alice', 'alice-secret', Scram::SCRAM_SHA_256])
            ->getMock();
        $provider->method('generateNonce')
            ->will($this->returnValue('5Fr49BaTHKn0i9ytDBMw8YXNMOemtxbJ+opDL/miWK8='));
        $provider->method('verifyMessage')
            ->will($this->returnValue(false));
        $socket = $this->getSocketForInvalidFinalMessage($message);

        $provider->authenticate($socket);
    }
}
