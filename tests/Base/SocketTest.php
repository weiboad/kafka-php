<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace KafkaTest\Base;

use \Kafka\Socket;
use \Kafka\Config;
use \KafkaTest\Base\StreamStub\Simple as SimpleStream;
use \KafkaTest\Base\StreamStub\Stream;
use org\bovigo\vfs\vfsStream;

/**
+------------------------------------------------------------------------------
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class SocketTest extends \PHPUnit\Framework\TestCase
{
    // {{{ consts
    // }}}
    // {{{ members

    private $root;

    // }}}
    // {{{ functions
    // {{{ public function setUp()

    public function setUp()
    {
        $this->root = vfsStream::setup('test', 0777, ['localKey' => 'data', 'localCert' => 'data', 'cafile' => 'data']);
    }

    // }}}
    // {{{ public function tearDown()

    public function tearDown()
    {
        $this->clearStreamMock();
    }

    // }}}
    // {{{ public function testCreateStreamHostName()

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

    // }}}
    // {{{ public function testCreateStreamPort()

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

    // }}}
    // {{{ public function testCreateStream()

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

    // }}}
    // {{{ public function testCreateStreamFailure()

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

    // }}}
    // {{{ public function testCreateStreamSsl()

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

    // }}}
    // {{{ public function testReadBlockingMaxRead()

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

    // }}}
    // {{{ public function testReadBlockingFailure()

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

    // }}}
    // {{{ public function testReadBlockingTimeout()

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

    // }}}
    // {{{ public function testReadBlockingTimeoutElse()

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

    // }}}
    // {{{ public function testReadBlockingReadFailure()

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

    // }}}
    // {{{ public function testWriteBlockingFailure()

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

    // }}}
    // {{{ public function testWriteBlockingTimeout()

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

    // }}}
    // {{{ public function testWriteBlockingTimeoutElse()

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

    // }}}
    // {{{ public function testWriteBlockingMaxBuffer()

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

    // }}}
    // {{{ public function testWriteBlockingReturnFalse()

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

    // }}}
    // {{{ private function mockStreamSocketClient()

    private function mockStreamSocketClient($host, $port, $config = null, $sasl = null, $mockMethod = [])
    {
        if (empty($mockMethod)) {
            $mockMethod = ['createSocket'];
        } else {
            $mockMethod = array_merge(['createSocket'], $mockMethod);
        }
        
        $socket = $this->getMockBuilder(Socket::class)
            ->setMethods($mockMethod)
            ->setConstructorArgs([$host, $port, $config, $sasl])
            ->getMock();

        $socket->method('createSocket')
               ->will($this->returnCallback(function ($remoteSocket, $context, &$errno, &$error) {
                    return @fopen($remoteSocket, 'r+', false, $context);
               }));
        return $socket;
    }

    // }}}
    // {{{ private function initStreamStub()

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

    // }}}
    // {{{ private function createStream()

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

    // }}}
    // {{{ private function clearStreamMock()
    
    private function clearStreamMock()
    {
        if (in_array('ssl', stream_get_wrappers(), true)) {
            stream_wrapper_unregister('ssl');
        }
        if (in_array('tcp', stream_get_wrappers(), true)) {
            stream_wrapper_unregister('tcp');
        }
    }

    // }}}
    // }}}
}
