<?php
namespace KafkaTest\Base;

use \Kafka\Socket;
use \Kafka\Config;
use \KafkaTest\Base\StreamStub\Simple as SimpleStream;
use \KafkaTest\Base\StreamStub\Stream;
use org\bovigo\vfs\vfsStream;

class SocketTest extends \PHPUnit\Framework\TestCase
{

    private $root;

    public function setUp()
    {
        $this->root = vfsStream::setup('test', 0777, ['localKey' => 'data', 'localCert' => 'data', 'cafile' => 'data']);
    }

    public function tearDown()
    {
        $this->clearStreamMock();
    }

    /**
     * testCreateStreamHostName
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Cannot open null host.
     * @access public
     * @return void
     */
    public function testCreateStreamHostName()
    {
        $socket = new Socket('', -99);
        $socket->connect();
    }

    /**
     * testCreateStreamPort
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Cannot open without port.
     * @access public
     * @return void
     */
    public function testCreateStreamPort()
    {
        $socket = new Socket('123', -99);
        $socket->connect();
    }

    /**
     * testCreateStream
     *
     * @access public
     * @return void
     */
    public function testCreateStream()
    {
        $host      = '127.0.0.1';
        $port      = 9192;
        $transport = 'tcp';

        $this->initStreamStub($transport, $host, $port);

        $sasl = $this->createMock(\Kafka\Sasl\Plain::class);
        $sasl->expects($this->once())
             ->method('authenticate')
             ->with($this->isInstanceOf(Socket::class));
        $socket = $this->mockStreamSocketClient($host, $port, null, $sasl);
        $socket->connect();
    }

    /**
     * testCreateStreamFailure
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Could not connect to 127.0.0.1:9192 ( [])
     * @access public
     * @return void
     */
    public function testCreateStreamFailure()
    {
        $host      = '127.0.0.1';
        $port      = 9192;
        $transport = 'tcp';

        $this->initStreamStub($transport, $host, $port, false);
        $socket = $this->mockStreamSocketClient($host, $port);
        $socket->connect();
    }

    /**
     * testCreateStreamSsl
     *
     * @access public
     * @return void
     */
    public function testCreateStreamSsl()
    {
        $host       = '127.0.0.1';
        $port       = 9192;
        $transport  = 'ssl';
        $localCert  = $this->root->url() . '/localCert';
        $localKey   = $this->root->url() . '/localKey';
        $verifyPeer = false;
        $passphrase = '123456';
        $cafile     = $this->root->url() . '/cafile';
        $peerName   = 'kafka';
        $context    = stream_context_create(['ssl' => [
                'local_cert' => $localCert,
                'local_pk' => $localKey,
                'verify_peer' => $verifyPeer,
                'passphrase' => $passphrase,
                'cafile' => $cafile,
                'peer_name' => $peerName
        ]]);
        

        $streamMock = $this->initStreamStub($transport, $host, $port, true);
        $streamMock->expects($this->once())
                   ->method('context')
                   ->with($this->equalTo(stream_context_get_options($context)));

        $config = $this->getMockForAbstractClass(\Kafka\Config::class);
        $config->setSslEnable(true);
        $config->setSslLocalPk($localKey);
        $config->setSslLocalCert($localCert);
        $config->setSslCafile($cafile);
        $config->setSslPassphrase($passphrase);
        $config->setSslVerifyPeer($verifyPeer);
        $config->setSslPeerName($peerName);

        $sasl = $this->createMock(\Kafka\Sasl\Plain::class);
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
     * @expectedExceptionMessage Invalid length given, it should be lesser than or equals to 5242880
     * @access public
     * @return void
     */
    public function testReadBlockingMaxRead()
    {
        $host      = '127.0.0.1';
        $port      = 9192;
        $transport = 'tcp';
        $socket    = $this->mockStreamSocketClient($host, $port);
        $socket->readBlocking(Socket::READ_MAX_LENGTH + 1);
    }

    /**
     * testReadBlockingFailure
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Could not read 4 bytes from stream (not readable)
     * @access public
     * @return void
     */
    public function testReadBlockingFailure()
    {
        $host      = '127.0.0.1';
        $port      = 9192;
        $transport = 'tcp';

        $this->initStreamStub($transport, $host, $port);
        $socket = $this->createStream($host, $port, false);
        $socket->readBlocking(4);
    }

    /**
     * testReadBlockingTimeout
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Timed out reading 4 bytes from stream
     * @access public
     * @return void
     */
    public function testReadBlockingTimeout()
    {
        $host       = '127.0.0.1';
        $port       = 9192;
        $transport  = 'tcp';
        $streamMock = $this->initStreamStub($transport, $host, $port);
        $streamMock->method('eof')
                   ->will($this->returnValue(false));
        $socket = $this->createStream($host, $port, 0, ['timed_out' => true]);
        $socket->readBlocking(4);
    }

    /**
     * testReadBlockingTimeoutElse
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Could not read 4 bytes from stream (not readable)
     * @access public
     * @return void
     */
    public function testReadBlockingTimeoutElse()
    {
        $host       = '127.0.0.1';
        $port       = 9192;
        $transport  = 'tcp';
        $streamMock = $this->initStreamStub($transport, $host, $port);
        $streamMock->method('eof')
                   ->will($this->returnValue(false));
        $socket = $this->createStream($host, $port, 0, []);
        $socket->connect();
        $socket->readBlocking(4);
    }

    /**
     * testReadBlockingReadFailure
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Unexpected EOF while reading 4 bytes from stream (no data)
     * @access public
     * @return void
     */
    public function testReadBlockingReadFailure()
    {
        $host      = '127.0.0.1';
        $port      = 9192;
        $transport = 'tcp';

        $streamMock = $this->initStreamStub($transport, $host, $port);
        $streamMock->method('eof')->will($this->returnValue(true));

        $socket = $this->createStream($host, $port, 1);
        $socket->readBlocking(4);
    }

    /**
     * testReadBlockingReadFailureTryTimeout
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Timed out while reading 4 bytes from socket, 4 bytes are still needed
     * @access public
     * @return void
     */
    public function testReadBlockingReadFailureTryTimeout()
    {
        $host      = '127.0.0.1';
        $port      = 9192;
        $transport = 'tcp';

        $streamMock = $this->initStreamStub($transport, $host, $port);
        $streamMock->method('eof')->will($this->returnValue(false));
        $streamMock->method('read')->will($this->returnValue(''));

        $socket = $this->createStream($host, $port, 1);
        $socket = $this->mockStreamSocketClient($host, $port, null, null, ['select']);
        $socket->expects($this->exactly(2))
               ->method('select')
            ->will($this->onConsecutiveCalls(
                1,
                0
            ));
        $socket->connect();
        $socket->readBlocking(4);
    }

    /**
     * testRecvTimeout
     *
     * @access public
     * @return void
     */
    public function testRecvTimeout()
    {
        $host       = '127.0.0.1';
        $port       = 9192;
        $transport  = 'tcp';
        $streamMock = $this->initStreamStub($transport, $host, $port);
        $streamMock->method('eof')
                   ->will($this->returnValue(false));
        $streamMock->method('read')
                   ->will($this->returnValue('xxxx'));
        $socket = $this->mockStreamSocketClient($host, $port, null, null, ['select']);
        $socket->setRecvTimeoutSec(3000);
        $socket->setRecvTimeoutUsec(30001);
        $socket->method('select')
               ->with($this->isType('array'), $this->equalTo(3000), $this->equalTo(30001), $this->equalTo(true))
               ->will($this->returnValue(1));
        $socket->connect();
        $data = $socket->readBlocking(4);
        $this->assertEquals('xxxx', $data);
    }

    /**
     * testWriteBlockingFailure
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Could not write 4 bytes to stream
     * @access public
     * @return void
     */
    public function testWriteBlockingFailure()
    {
        $host      = '127.0.0.1';
        $port      = 9192;
        $transport = 'tcp';

        $this->initStreamStub($transport, $host, $port);
        $socket = $this->createStream($host, $port, false);
        $socket->connect();
        $socket->writeBlocking('test');
    }

    /**
     * testWriteBlockingTimeout
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Timed out writing 1 bytes to stream after writing 0 bytes
     * @access public
     * @return void
     */
    public function testWriteBlockingTimeout()
    {
        $host       = '127.0.0.1';
        $port       = 9192;
        $transport  = 'tcp';
        $streamMock = $this->initStreamStub($transport, $host, $port);
        $streamMock->method('eof')
                   ->will($this->returnValue(false));
        $socket = $this->createStream($host, $port, 0, ['timed_out' => true]);
        $socket->writeBlocking(4);
    }

    /**
     * testWriteBlockingTimeoutElse
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Could not write 4 bytes to stream
     * @access public
     * @return void
     */
    public function testWriteBlockingTimeoutElse()
    {
        $host       = '127.0.0.1';
        $port       = 9192;
        $transport  = 'tcp';
        $streamMock = $this->initStreamStub($transport, $host, $port);
        $streamMock->method('eof')
                   ->will($this->returnValue(false));
        $socket = $this->createStream($host, $port, 0, []);
        $socket->writeBlocking('xxxx');
    }

    /**
     * testWriteBlockingMaxBuffer
     *
     * @access public
     * @return void
     */
    public function testWriteBlockingMaxBuffer()
    {
        $str       = str_pad('', Socket::MAX_WRITE_BUFFER * 2, "*");
        $host      = '127.0.0.1';
        $port      = 9192;
        $transport = 'tcp';

        $streamMock = $this->initStreamStub($transport, $host, $port);
        $streamMock->method('write')->will($this->onConsecutiveCalls(
            Socket::MAX_WRITE_BUFFER,
            Socket::MAX_WRITE_BUFFER
        ))->withConsecutive(
            [substr($str, 0, Socket::MAX_WRITE_BUFFER)],
            [substr($str, Socket::MAX_WRITE_BUFFER)]
        );

        $socket = $this->createStream($host, $port, 1);
        $this->assertEquals(Socket::MAX_WRITE_BUFFER * 2, $socket->writeBlocking($str));
    }

    /**
     * testWriteBlockingReturnFalse
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage After 4 attempts could not write 4096 bytes to stream, completed writing only 0 bytes
     * @access public
     * @return void
     */
    public function testWriteBlockingReturnFalse()
    {
        $str       = str_pad('', Socket::MAX_WRITE_BUFFER * 2, "*");
        $host      = '127.0.0.1';
        $port      = 9192;
        $transport = 'tcp';

        $streamMock = $this->initStreamStub($transport, $host, $port);
        $streamMock->method('write')->will($this->onConsecutiveCalls(
            0
        ))->withConsecutive(
            [substr($str, 0, Socket::MAX_WRITE_BUFFER)]
        );

        $socket = $this->createStream($host, $port, 1);
        $socket->writeBlocking($str);
    }

    /**
     * testSendTimeout
     *
     * @access public
     * @return void
     */
    public function testSendTimeout()
    {
        $host       = '127.0.0.1';
        $port       = 9192;
        $transport  = 'tcp';
        $streamMock = $this->initStreamStub($transport, $host, $port);
        $streamMock->method('eof')
                   ->will($this->returnValue(false));
        $streamMock->method('write')
                   ->will($this->returnValue(4));
        $socket = $this->mockStreamSocketClient($host, $port, null, null, ['select']);
        $socket->setSendTimeoutSec(3000);
        $socket->setSendTimeoutUsec(30001);
        $socket->method('select')
               ->with($this->isType('array'), $this->equalTo(3000), $this->equalTo(30001), $this->equalTo(false))
               ->will($this->returnValue(1));
        $socket->connect();
        $data = $socket->writeBlocking('xxxx');
        $this->assertEquals(4, $data);
    }

    private function mockStreamSocketClient($host, $port, $config = null, $sasl = null, $mockMethod = [])
    {
        if (empty($mockMethod)) {
            $mockMethod = ['createSocket'];
        } else {
            $mockMethod = array_merge(['createSocket'], $mockMethod);
        }
        
        $socket       = $this->getMockBuilder(Socket::class)
            ->setMethods($mockMethod)
            ->setConstructorArgs([$host, $port, $config, $sasl])
            ->getMock();
        $socket->loop = \Kafka\Loop::getInstance();

        $socket->method('createSocket')
               ->will($this->returnCallback(function ($remoteSocket, $context, &$errno, &$error) {
                    return @fopen($remoteSocket, 'r+', false, $context);
               }));
        return $socket;
    }

    private function initStreamStub($transport, $host, $port, $success = true)
    {
        $uri = sprintf('%s://%s:%s', $transport, $host, $port);
        stream_wrapper_register($transport, SimpleStream::class);
        $streamMock = $this->createMock(Stream::class);
        $streamMock->method('open')->with(
            $this->equalTo($uri),
            $this->equalTo('r+'),
            $this->equalTo(0)
        )->will($this->returnValue($success));
        $streamMock->method('option')->will($this->returnValue(true));
        SimpleStream::setMock($streamMock);
        return $streamMock;
    }

    private function createStream($host, $port, $select, $metaData = [])
    {
        $socket = $this->mockStreamSocketClient($host, $port, null, null, ['select', 'getMetaData']);
        $socket->method('select')
               ->will($this->returnValue($select));
        $socket->method('getMetaData')
               ->will($this->returnValue($metaData));
        $socket->connect();
        return $socket;
    }

    
    private function clearStreamMock()
    {
        if (in_array('ssl', stream_get_wrappers(), true)) {
            stream_wrapper_unregister('ssl');
        }
        if (in_array('tcp', stream_get_wrappers(), true)) {
            stream_wrapper_unregister('tcp');
        }
    }
}
