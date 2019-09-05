<?php
declare(strict_types=1);

namespace KafkaTest\Base;

use Kafka\Config;
use Kafka\SaslMechanism;
use Kafka\Socket;
use KafkaTest\Base\StreamStub\Simple as SimpleStream;
use KafkaTest\Base\StreamStub\Stream;
use org\bovigo\vfs\vfsStream;
use org\bovigo\vfs\vfsStreamDirectory;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use function array_merge;
use function fopen;
use function in_array;
use function sprintf;
use function str_pad;
use function stream_context_create;
use function stream_context_get_options;
use function stream_get_wrappers;
use function stream_wrapper_register;
use function stream_wrapper_unregister;
use function substr;

class SocketTest extends TestCase
{
    /**
     * @var vfsStreamDirectory
     */
    private $root;

    public function setUp(): void
    {
        $this->root = vfsStream::setup('test', 0777, ['localKey' => 'data', 'localCert' => 'data', 'cafile' => 'data']);
    }

    public function tearDown(): void
    {
        $this->clearStreamMock();
    }

    /**
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Cannot open null host.
     */
    public function testCreateStreamHostName(): void
    {
        $socket = new Socket('', -99);
        $socket->connect();
    }

    /**
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Cannot open without port.
     */
    public function testCreateStreamPort(): void
    {
        $socket = new Socket('123', -99);
        $socket->connect();
    }

    public function testCreateStream(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $this->initStreamStub('tcp', $host, $port);

        $sasl = $this->createMock(SaslMechanism::class);
        $sasl->expects($this->once())
             ->method('authenticate')
             ->with($this->isInstanceOf(Socket::class));

        $socket = $this->mockStreamSocketClient($host, $port, null, $sasl);
        $socket->connect();
    }

    /**
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Could not connect to 127.0.0.1:9192 (my custom error [99])
     */
    public function testCreateStreamFailure(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $this->initStreamStub('tcp', $host, $port, false);

        $socket = $this->mockStreamSocketClient($host, $port);
        $socket->connect();
    }

    public function testCreateStreamSsl(): void
    {
        $host       = '127.0.0.1';
        $port       = 9192;
        $verifyPeer = false;
        $cafile     = $this->root->url() . '/cafile';
        $peerName   = 'kafka';

        $context = stream_context_create(
            [
                'ssl' => [
                    'verify_peer' => $verifyPeer,
                    'cafile'      => $cafile,
                    'peer_name'   => $peerName,
                ],
            ]
        );

        $streamMock = $this->initStreamStub('ssl', $host, $port);

        $streamMock->expects($this->once())
                   ->method('context')
                   ->with(stream_context_get_options($context));

        $config = $this->getMockForAbstractClass(Config::class);
        $config->setSslEnable(true);
        $config->setSslEnableAuthentication(false);
        $config->setSslCafile($cafile);
        $config->setSslVerifyPeer($verifyPeer);
        $config->setSslPeerName($peerName);

        $sasl = $this->createMock(SaslMechanism::class);
        $sasl->expects($this->once())
             ->method('authenticate')
             ->with($this->isInstanceOf(Socket::class));

        $socket = $this->mockStreamSocketClient($host, $port, $config, $sasl);
        $socket->connect();
    }

    public function testCreateStreamSslAuthentication(): void
    {
        $host       = '127.0.0.1';
        $port       = 9192;
        $localCert  = $this->root->url() . '/localCert';
        $localKey   = $this->root->url() . '/localKey';
        $verifyPeer = false;
        $passphrase = '123456';
        $cafile     = $this->root->url() . '/cafile';
        $peerName   = 'kafka';

        $context = stream_context_create(
            [
                'ssl' => [
                    'local_cert'  => $localCert,
                    'local_pk'    => $localKey,
                    'verify_peer' => $verifyPeer,
                    'passphrase'  => $passphrase,
                    'cafile'      => $cafile,
                    'peer_name'   => $peerName,
                ],
            ]
        );

        $streamMock = $this->initStreamStub('ssl', $host, $port);

        $streamMock->expects($this->once())
                   ->method('context')
                   ->with(stream_context_get_options($context));

        $config = $this->getMockForAbstractClass(Config::class);
        $config->setSslEnable(true);
        $config->setSslEnableAuthentication(true);
        $config->setSslLocalPk($localKey);
        $config->setSslLocalCert($localCert);
        $config->setSslCafile($cafile);
        $config->setSslPassphrase($passphrase);
        $config->setSslVerifyPeer($verifyPeer);
        $config->setSslPeerName($peerName);

        $sasl = $this->createMock(SaslMechanism::class);
        $sasl->expects($this->once())
             ->method('authenticate')
             ->with($this->isInstanceOf(Socket::class));

        $socket = $this->mockStreamSocketClient($host, $port, $config, $sasl);
        $socket->connect();
    }

    /**
     * testReadBlockingMaxRead
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Invalid length 5242881 given, it should be lesser than or equals to 5242880
     */
    public function testReadBlockingMaxRead(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $socket = $this->mockStreamSocketClient($host, $port);
        $socket->readBlocking(Socket::READ_MAX_LENGTH + 1);
    }

    /**
     * testReadBlockingFailure
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Could not read 4 bytes from stream (not readable)
     */
    public function testReadBlockingFailure(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $this->initStreamStub('tcp', $host, $port);

        $socket = $this->createStream($host, $port, false);
        $socket->readBlocking(4);
    }

    /**
     * testReadBlockingTimeout
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Timed out reading 4 bytes from stream
     */
    public function testReadBlockingTimeout(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $streamMock = $this->initStreamStub('tcp', $host, $port);
        $streamMock->method('eof')->willReturn(false);

        $socket = $this->createStream($host, $port, 0, ['timed_out' => true]);
        $socket->readBlocking(4);
    }

    /**
     * testReadBlockingTimeoutElse
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Could not read 4 bytes from stream (not readable)
     */
    public function testReadBlockingTimeoutElse(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $streamMock = $this->initStreamStub('tcp', $host, $port);
        $streamMock->method('eof')->willReturn(false);

        $socket = $this->createStream($host, $port, 0);
        $socket->connect();
        $socket->readBlocking(4);
    }

    /**
     * testReadBlockingReadFailure
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Unexpected EOF while reading 4 bytes from stream (no data)
     */
    public function testReadBlockingReadFailure(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $streamMock = $this->initStreamStub('tcp', $host, $port);
        $streamMock->method('eof')->will($this->returnValue(true));

        $socket = $this->createStream($host, $port, 1);
        $socket->readBlocking(4);
    }

    /**
     * testReadBlockingReadFailureTryTimeout
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Timed out while reading 4 bytes from stream, 4 bytes are still needed
     */
    public function testReadBlockingReadFailureTryTimeout(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $streamMock = $this->initStreamStub('tcp', $host, $port);
        $streamMock->method('eof')->will($this->returnValue(false));
        $streamMock->method('read')->will($this->returnValue(''));

        $socket = $this->mockStreamSocketClient($host, $port, null, null, ['select']);
        $socket->expects($this->exactly(2))
               ->method('select')
               ->willReturnOnConsecutiveCalls(1, 0);

        $socket->connect();
        $socket->readBlocking(4);
    }

    public function testRecvTimeout(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $streamMock = $this->initStreamStub('tcp', $host, $port);
        $streamMock->method('eof')->willReturn(false);
        $streamMock->method('read')->willReturn('xxxx');

        $socket = $this->mockStreamSocketClient($host, $port, null, null, ['select']);
        $socket->setRecvTimeoutSec(3000);
        $socket->setRecvTimeoutUsec(30001);

        $socket->method('select')
               ->with($this->isType('array'), 3000, 30001, true)
               ->willReturn(1);

        $socket->connect();

        $this->assertEquals('xxxx', $socket->readBlocking(4));
    }

    /**
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Could not write 4 bytes to stream
     */
    public function testWriteBlockingFailure(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $this->initStreamStub('tcp', $host, $port);
        $socket = $this->createStream($host, $port, false);
        $socket->connect();
        $socket->writeBlocking('test');
    }

    /**
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Timed out writing 1 bytes to stream after writing 0 bytes
     */
    public function testWriteBlockingTimeout(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $streamMock = $this->initStreamStub('tcp', $host, $port);
        $streamMock->method('eof')
                   ->will($this->returnValue(false));
        $socket = $this->createStream($host, $port, 0, ['timed_out' => true]);
        $socket->writeBlocking('4');
    }

    /**
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Could not write 4 bytes to stream
     */
    public function testWriteBlockingTimeoutElse(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $streamMock = $this->initStreamStub('tcp', $host, $port);
        $streamMock->method('eof')
                   ->will($this->returnValue(false));
        $socket = $this->createStream($host, $port, 0, []);
        $socket->writeBlocking('xxxx');
    }

    public function testWriteBlockingMaxBuffer(): void
    {
        $str  = str_pad('', Socket::MAX_WRITE_BUFFER * 2, '*');
        $host = '127.0.0.1';
        $port = 9192;

        $streamMock = $this->initStreamStub('tcp', $host, $port);

        $streamMock->method('write')
                   ->withConsecutive([substr($str, 0, Socket::MAX_WRITE_BUFFER)], [substr($str, Socket::MAX_WRITE_BUFFER)])
                   ->willReturnOnConsecutiveCalls(Socket::MAX_WRITE_BUFFER, Socket::MAX_WRITE_BUFFER);

        $socket = $this->createStream($host, $port, 1);
        $this->assertEquals(Socket::MAX_WRITE_BUFFER * 2, $socket->writeBlocking($str));
    }

    /**
     * testWriteBlockingReturnFalse
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage After 4 attempts could not write 4096 bytes to stream, completed writing only 0 bytes
     */
    public function testWriteBlockingReturnFalse(): void
    {
        $str  = str_pad('', Socket::MAX_WRITE_BUFFER * 2, '*');
        $host = '127.0.0.1';
        $port = 9192;

        $streamMock = $this->initStreamStub('tcp', $host, $port);

        $streamMock->method('write')
                   ->withConsecutive([substr($str, 0, Socket::MAX_WRITE_BUFFER)])
                   ->willReturnOnConsecutiveCalls(0);

        $socket = $this->createStream($host, $port, 1);
        $socket->writeBlocking($str);
    }

    public function testSendTimeout(): void
    {
        $host = '127.0.0.1';
        $port = 9192;

        $streamMock = $this->initStreamStub('tcp', $host, $port);
        $streamMock->method('eof')->willReturn(false);
        $streamMock->method('write')->willReturn(4);

        $socket = $this->mockStreamSocketClient($host, $port, null, null, ['select']);
        $socket->setSendTimeoutSec(3000);
        $socket->setSendTimeoutUsec(30001);

        $socket->method('select')
               ->with($this->isType('array'), 3000, 30001, false)
               ->willReturn(1);

        $socket->connect();

        $this->assertEquals(4, $socket->writeBlocking('xxxx'));
    }

    /**
     * @param string[] $mockMethod
     *
     * @return Socket|MockObject
     */
    private function mockStreamSocketClient(
        string $host,
        int $port,
        ?Config $config = null,
        ?SaslMechanism $sasl = null,
        array $mockMethod = []
    ): Socket {
        $socket = $this->getMockBuilder(Socket::class)
                       ->setMethods(array_merge(['createSocket'], $mockMethod))
                       ->setConstructorArgs([$host, $port, $config, $sasl])
                       ->getMock();

        $socket->method('createSocket')
            ->willReturnCallback(
                function (string $remoteSocket, $context, ?int &$errno, ?string &$errstr) {
                    $errno  = 99;
                    $errstr = 'my custom error';

                    return @fopen($remoteSocket, 'r+', false, $context);
                }
            );

        return $socket;
    }

    /**
     * @return Stream|MockObject
     */
    private function initStreamStub(string $transport, string $host, int $port, bool $success = true): Stream
    {
        $uri = sprintf('%s://%s:%s', $transport, $host, $port);
        stream_wrapper_register($transport, SimpleStream::class);

        $streamMock = $this->createMock(Stream::class);
        $streamMock->method('open')->with($uri, 'r+', 0)->willReturn($success);
        $streamMock->method('option')->willReturn(true);

        SimpleStream::setMock($streamMock);

        return $streamMock;
    }

    /**
     * @param int|bool $select
     * @param mixed[]  $metaData
     *
     * @return Socket|MockObject
     */
    private function createStream(string $host, int $port, $select, array $metaData = []): Socket
    {
        $socket = $this->mockStreamSocketClient($host, $port, null, null, ['select', 'getMetaData']);

        $socket->method('select')
               ->willReturn($select);

        $socket->method('getMetaData')
               ->willReturn($metaData);

        $socket->connect();

        return $socket;
    }

    private function clearStreamMock(): void
    {
        if (in_array('ssl', stream_get_wrappers(), true)) {
            stream_wrapper_unregister('ssl');
        }

        if (in_array('tcp', stream_get_wrappers(), true)) {
            stream_wrapper_unregister('tcp');
        }
    }
}
