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
use \KafkaTest\Base\Stream\Simple as SimpleStream;
use \KafkaTest\Base\Stream\StreamMock;

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
    // }}}
    // {{{ functions
    // {{{ public function tearDown()

    public function tearDown()  
    {
        $this->clearStreamMock();
    }

    // }}}
    // {{{ public function testConstruct()

    /**
     * testConstruct
     *
     * @access public
     * @return void
     */
    public function testConstruct()
    {
        $host = '127.0.0.1';
        $port = 9092;
        $socket = new Socket($host, $port);
        $this->assertSame($host, $socket->getHost());
        $this->assertSame($port, $socket->getPort());
        $this->assertNull($socket->getConfig());
        $this->assertNull($socket->getSaslProvider());

        $config = $this->getMockForAbstractClass(Config::class);
        $socket->setConfig($config);
        $this->assertEquals($config, $socket->getConfig());
        
        
        $sasl = new \Kafka\Sasl\Plain('username', 'password');
        $socket->setSaslProvider($sasl);
        $this->assertEquals($sasl, $socket->getSaslProvider());

        $socket->setSendTimeoutSec(200);
        $this->assertEquals(200, $socket->getSendTimeoutSec());
        $socket->setSendTimeoutUsec(330.11);
        $this->assertEquals(330.11, $socket->getSendTimeoutUsec());
        $socket->setRecvTimeoutSec(130.11);
        $this->assertEquals(130.11, $socket->getRecvTimeoutSec());
        $socket->setRecvTimeoutUsec(430.11);
        $this->assertEquals(430.11, $socket->getRecvTimeoutUsec());
        $socket->setMaxWriteAttempts(3);
        $this->assertEquals(3, $socket->getMaxWriteAttempts());

        $this->assertNull($socket->getSocket());
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
        \uopz_flags(Socket::class, 'createStream', ZEND_ACC_PUBLIC);
        $socket = new Socket('', -99);
        $socket->createStream();
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
        \uopz_flags(Socket::class, 'createStream', ZEND_ACC_PUBLIC);
        $socket = new Socket('123', -99);
        $socket->createStream();
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
        $host = '127.0.0.1';
        $port = 9192;
        $transport = 'tcp';

        $this->initStreamMock($transport, $host, $port);

        $this->mockStreamSocketClient($transport, $host, $port);

        \uopz_flags(\Kafka\Sasl\Plain::class, null, 0);
        \uopz_flags(Socket::class, 'createStream', ZEND_ACC_PUBLIC);
        $sasl = $this->createMock(\Kafka\Sasl\Plain::class);
        $sasl->expects($this->once())
             ->method('authenticate')
             ->with($this->isInstanceOf(Socket::class));
        
        $socket = new Socket($host, $port);
        $socket->setSaslProvider($sasl);
        $stream = $socket->createStream();
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
        $host = '127.0.0.1';
        $port = 9192;
        $transport = 'tcp';

        $this->initStreamMock($transport, $host, $port, null, false);

        $this->mockStreamSocketClient($transport, $host, $port);

        \uopz_flags(\Kafka\Sasl\Plain::class, null, 0);
        \uopz_flags(Socket::class, 'createStream', ZEND_ACC_PUBLIC);
        $socket = new Socket($host, $port);
        $stream = $socket->createStream();
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
        $host = '127.0.0.1';
        $port = 9192;
        $transport = 'ssl';
        $localCert    = '/etc/testcert';
        $localKey     = 'etc/localkey';
        $verifyPeer   = false;
        $passphrase   = '123456';
        $cafile       = '/etc/cafile';
        $peerName     = 'kafka';
        $context = stream_context_create(['ssl' => [
                'local_cert' => $localCert,
                'local_pk' => $localKey,
                'verify_peer' => $verifyPeer,
                'passphrase' => $passphrase,
                'cafile' => $cafile,
                'peer_name' => $peerName
        ]]);

        $streamMock = $this->initStreamMock($transport, $host, $port, $context);
        $streamMock->expects($this->once())
                   ->method('context')
                   ->with($this->equalTo($context));

        $this->mockStreamSocketClient($transport, $host, $port, $context);

        \uopz_flags(\Kafka\Sasl\Plain::class, null, 0);
        \uopz_flags(Socket::class, 'createStream', ZEND_ACC_PUBLIC);
        $sasl = $this->createMock(\Kafka\Sasl\Plain::class);
        $sasl->expects($this->once())
             ->method('authenticate')
             ->with($this->isInstanceOf(Socket::class));
        $config = $this->getMockForAbstractClass(\Kafka\Config::class);
        $config->setSslEnable(true);
        
        $socket = new Socket($host, $port, $config, $sasl);
        $stream = $socket->createStream();
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
        $host = '127.0.0.1';
        $port = 9192;
        $transport = 'tcp';
        $socket = new Socket($host, $port);
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
        $socket = $this->createStream();
        \uopz_set_return('stream_select', false);
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
        $socket = $this->createStream();
        \uopz_set_return('stream_select', 0);
        \uopz_set_return('stream_get_meta_data', array('timed_out' => true));
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
        $socket = $this->createStream();
        \uopz_set_return('stream_select', 0);
        \uopz_set_return('stream_get_meta_data', array());
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
        $host = '127.0.0.1';
        $port = 9192;
        $transport = 'tcp';

        $streamMock = $this->initStreamMock($transport, $host, $port);
        $streamMock->method('eof')->will($this->returnValue(true));

        $this->mockStreamSocketClient($transport, $host, $port);
        \uopz_flags(Socket::class, 'createStream', ZEND_ACC_PUBLIC);

        \uopz_set_return('stream_select', 1);
        $socket = new Socket($host, $port);
        $socket->createStream();
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
        $socket = $this->createStream();
        \uopz_set_return('stream_select', false);
        $socket->writeBlocking('test');
    }

    // }}}
    // {{{ public function testWriteBlockingTimeout()

    /**
     * testWriteBlockingTimeout
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Timed out writing 4 bytes to stream after writing 0 bytes
     * @access public
     * @return void
     */
    public function testWriteBlockingTimeout()
    {
        $socket = $this->createStream();
        \uopz_set_return('stream_select', 0);
        \uopz_set_return('stream_get_meta_data', array('timed_out' => true));
        $socket->writeBlocking('test');
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
        $socket = $this->createStream();
        \uopz_set_return('stream_select', 0);
        \uopz_set_return('stream_get_meta_data', array());
        $socket->writeBlocking('test');
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
        $str = str_pad('',  Socket::MAX_WRITE_BUFFER * 2, "*");
        $host = '127.0.0.1';
        $port = 9192;
        $transport = 'tcp';

        $streamMock = $this->initStreamMock($transport, $host, $port);
        $streamMock->method('write')->will($this->onConsecutiveCalls(
            Socket::MAX_WRITE_BUFFER,
            Socket::MAX_WRITE_BUFFER
        ))->withConsecutive(
            [substr($str, 0, Socket::MAX_WRITE_BUFFER)],
            [substr($str, Socket::MAX_WRITE_BUFFER)]
        );

        $this->mockStreamSocketClient($transport, $host, $port);
        \uopz_flags(Socket::class, 'createStream', ZEND_ACC_PUBLIC);

        \uopz_set_return('stream_select', 1);
        $socket = new Socket($host, $port);
        $socket->createStream();
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
        $str = str_pad('',  Socket::MAX_WRITE_BUFFER * 2, "*");
        $host = '127.0.0.1';
        $port = 9192;
        $transport = 'tcp';

        $streamMock = $this->initStreamMock($transport, $host, $port);
        $streamMock->method('write')->will($this->onConsecutiveCalls(
            0 
        ))->withConsecutive(
            [substr($str, 0, Socket::MAX_WRITE_BUFFER)]
        );

        $this->mockStreamSocketClient($transport, $host, $port);
        \uopz_flags(Socket::class, 'createStream', ZEND_ACC_PUBLIC);

        \uopz_set_return('stream_select', 1);
        $socket = new Socket($host, $port);
        $socket->createStream();
        $socket->writeBlocking($str);
    }

    // }}}
    // {{{ private function mockStreamSocketClient()

    private function mockStreamSocketClient($transport, $host, $port, $context = null)
    {
        if ($context == null) {
            $context = stream_context_create([]);
        }
        $uri = sprintf('%s://%s:%s', $transport, $host, $port);
        $stream = @fopen($uri, 'r+', false, $context);
        \uopz_set_return('stream_socket_client', $stream);
    }

    // }}}
    // {{{ private function initStreamMock()

    private function initStreamMock($transport, $host, $port, $context = null, $success = true)
    {
        $uri = sprintf('%s://%s:%s', $transport, $host, $port);
        if ($context == null) {
            $context = stream_context_create([]);
        }
        stream_wrapper_register($transport, SimpleStream::class);
        $streamMock = $this->createMock(StreamMock::class);
        $streamMock->method('open')->with(
            $this->equalTo($uri),
            $this->equalTo('r+'),
            $this->equalTo(0)
        )->will($this->returnValue($success));
        SimpleStream::setMock($streamMock);
        return $streamMock;
    }

    // }}}
    // {{{ private function createStream()

    private function createStream()
    {
        $host = '127.0.0.1';
        $port = 9192;
        $transport = 'tcp';

        $streamMock = $this->initStreamMock($transport, $host, $port);
        $this->mockStreamSocketClient($transport, $host, $port);
        \uopz_flags(Socket::class, 'createStream', ZEND_ACC_PUBLIC);

        $socket = new Socket($host, $port);
        $socket->createStream();
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

        if (\uopz_get_return('stream_socket_client')) {
            \uopz_unset_return('stream_socket_client');
        }
        if (\uopz_get_return('stream_select')) {
            \uopz_unset_return('stream_select');
        }
        if (\uopz_get_return('stream_get_meta_data')) {
            \uopz_unset_return('stream_get_meta_data');
        }
    }

    // }}} 
    // }}}
}
