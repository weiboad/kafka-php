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

namespace KafkaTest\Protocol;

use \Kafka\Protocol\Decoder;

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

class DecoderTest extends \PHPUnit_Framework_TestCase
{
    // {{{ consts
    // }}}
    // {{{ members

    /**
     * stream
     *
     * @var mixed
     * @access protected
     */
    protected $stream = null;

    // }}}
    // {{{ functions
    // {{{ public function setUp()

    /**
     * setUp
     *
     * @access public
     * @return void
     */
    public function setUp()
    {
        $this->stream = \Kafka\Socket::createFromStream(fopen('php://temp', 'w+b'));
    }

    // }}}
    // {{{ public function setData()

    /**
     * getData
     *
     * @access public
     * @param $data
     */
    public function setData($data)
    {
        $len = $this->stream->write($data, true);
        $this->stream->rewind();
        return $len;
    }

    // }}}
    // {{{ public function testFetchRequest()

    /**
     * testFetchRequest
     *
     * @access public
     * @return void
     */
    public function testFetchRequest()
    {
        $this->setData(Decoder::Khex2bin('0000007200000000000000010004746573740000000100000000000000000000000000630000004e000000000000006100000020d4091dc7000000000000000000123332333231603160606060606060606060600000000000000062000000166338ddac000000000000000000086d65737361676532'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        $actual  = $decoder->fetchResponse();
        $this->assertInstanceOf('\Kafka\Protocol\Fetch\Topic', $actual);

        $this->assertEquals(1, count($actual));
    }

    // }}}
    //{{{ public function testProduceResponse()

    /**
     * testProduceResponse
     *
     * @access public
     * @return void
     */
    public function testProduceResponse()
    {
        $this->setData(Decoder::Khex2bin('00000047000000000000000200057465737436000000020000000200000000000000000034000000050000000000000000002800047465737400000001000000000000000000000000005f'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        $actual  = $decoder->produceResponse();

        $expect = array(
            'test6' => array(
                2 => array(
                    'errCode' => 0,
                    'offset'  => 52,
                ),
                5 => array(
                    'errCode' => 0,
                    'offset'  => 40,
                ),
            ),
            'test' => array(
                0 => array(
                    'errCode' => 0,
                    'offset'  => 95,
                ),
            ),
        );
        $this->assertEquals($expect, $actual);
    }

    // }}}
    //{{{ public function testProduceResponseNotData()

    /**
     * testProduceResponseNotData
     *
     * @access public
     * @return void
     */
    public function testProduceResponseNotData()
    {
        $this->setData(Decoder::Khex2bin('00000000'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        try {
            $decoder->produceResponse();
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('produce response invalid.', $e->getMessage());
        }
    }

    // }}}
    //{{{ public function testProduceResponseWithErrorCodes()

    /**
     * testProduceResponseWithErrorCodes
     *
     * @access public
     * @return void
     */
    public function testProduceResponseWithErrorCodes()
    {
        // Break down of our encoded data
        // '00000047 00000000 00000002 0005 7465737436 00000002 00000002 000A 0000000000000034 00000005 FFFF 0000000000000028 0004 74657374 00000001 00000000 0000 000000000000005f'

        // 00000047  = data length = 47 bytes
        // 00000000 = ??
        // 00000002 = topic count: "2"

        // -- first topic --
        // 0005 = topic name length: 5 bytes
        // 7465737436 = "test6"
        // 00000002 = partition count: "2"
        // -- first topic - first partition --
        // 00000002 = partition id: "2"
        // 000A = error code: "10"
        // 0000000000000034 = partition offset: "52"
        // -- first topic - second partition --
        // 00000005 = partition id: "5"
        // 0000 = error code: "-1"
        // 0000000000000028 = offset: "40"

        // -- 2nd topic --
        // 0004 = topic name length: 4 bytes
        // 74657374 = "test"
        // 00000001 = partition count: "1"
        // -- 2nd topic - first partition --
        // 00000000 = partition id: "0"
        // 0000 = error code: "0"
        // 000000000000005f = offset: "95"

        $this->setData(Decoder::Khex2bin('000000470000000000000002000574657374360000000200000002000A000000000000003400000005FFFF000000000000002800047465737400000001000000000000000000000000005f'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        $actual  = $decoder->produceResponse();

        $expect = array(
            'test6' => array(
                2 => array(
                    'errCode' => 10,
                    'offset'  => 52,
                ),
                5 => array(
                    'errCode' => -1,
                    'offset'  => 40,
                ),
            ),
            'test' => array(
                0 => array(
                    'errCode' => 0,
                    'offset'  => 95,
                ),
            ),
        );
        $this->assertEquals($expect, $actual);
    }

    // }}}
    //{{{ public function testMetadataResponse()

    /**
     * testMetadataResponse
     *
     * @access public
     * @return void
     */
    public function testMetadataResponse()
    {
        $this->setData(Decoder::Khex2bin('0000007100000000000000030000000000086861646f6f703131000023840000000100086861646f6f703132000023840000000200086861646f6f70313300002384000000010000000574657374310000000100000000000000000002000000030000000000000001000000020000000100000002'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        $actual  = $decoder->metadataResponse();

        $expect = array(
            'brokers' => array(
                array(
                    'host' => 'hadoop11',
                    'port' => 9092,
                ),
                array(
                    'host' => 'hadoop12',
                    'port' => 9092,
                ),
                array(
                    'host' => 'hadoop13',
                    'port' => 9092,
                ),
            ),
            'topics' => array(
                'test1' => array(
                    'errCode' => 0,
                    'partitions'  => array(
                        0 => array(
                            'errCode'  => 0,
                            'leader'   => 2,
                            'replicas' => array(0, 1, 2),
                            'isr'      => array(2),
                        ),
                    ),
                ),
            ),
        );
        $this->assertEquals($expect, $actual);
    }

    // }}}
    //{{{ public function testMetadataResponseNotData()

    /**
     * testMetadataResponseNotData
     *
     * @access public
     * @return void
     */
    public function testMetadataResponseNotData()
    {
        $this->setData(Decoder::Khex2bin('00000000'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        try {
            $decoder->metadataResponse();
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('metaData response invalid.', $e->getMessage());
        }
    }

    // }}}
    //{{{ public function testOffsetResponse()

    /**
     * testOffsetResponse
     *
     * @access public
     * @return void
     */
    public function testOffsetResponse()
    {
        $this->setData(Decoder::Khex2bin('00000024000000000000000100047465737400000001000000000000000000010000000000000063'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        $actual  = $decoder->offsetResponse();

        $expect = array(
            'test' => array(
                0 => array(
                    'errCode' => 0,
                    'offset' => array(99),
                ),
            ),
        );
        $this->assertEquals($expect, $actual);
    }

    // }}}
    //{{{ public function testOffsetResponseNotData()

    /**
     * testOffsetResponseNotData
     *
     * @access public
     * @return void
     */
    public function testOffsetResponseNotData()
    {
        $this->setData(Decoder::Khex2bin('00000000'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        try {
            $decoder->offsetResponse();
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('offset response invalid.', $e->getMessage());
        }
    }

    // }}}
    //{{{ public function testCommitOffsetResponse()

    /**
     * testCommitOffsetResponse
     *
     * @access public
     * @return void
     */
    public function testCommitOffsetResponse()
    {
        $this->setData(Decoder::Khex2bin('0000001900000000000000010005746573743600000001000000020000'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        $actual  = $decoder->commitOffsetResponse();

        $expect = array(
            'test6' => array(
                2 => array(
                    'errCode' => 0,
                ),
            ),
        );
        $this->assertEquals($expect, $actual);
    }

    // }}}
    //{{{ public function testCommitOffsetResponseNotData()

    /**
     * testCommitOffsetResponseNotData
     *
     * @access public
     * @return void
     */
    public function testCommitOffsetResponseNotData()
    {
        $this->setData(Decoder::Khex2bin('00000000'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        try {
            $decoder->commitOffsetResponse();
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('commit offset response invalid.', $e->getMessage());
        }
    }

    // }}}
    //{{{ public function testFetchOffsetResponse()

    /**
     * testFetchOffsetResponse
     *
     * @access public
     * @return void
     */
    public function testFetchOffsetResponse()
    {
        $this->setData(Decoder::Khex2bin('000000230000000000000001000574657374360000000100000002000000000000000200000000'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        $actual  = $decoder->fetchOffsetResponse();

        $expect = array(
            'test6' => array(
                2 => array(
                    'offset'   => 2,
                    'metadata' => '',
                    'errCode'  => 0,
                ),
            ),
        );
        $this->assertEquals($expect, $actual);
    }

    // }}}
    //{{{ public function testFetchOffsetResponseErrorCode3()

    public function testFetchOffsetResponseErrorCode3()
    {
        // 00000023  = data length = 35 bytes
        // 00000000 = ??
        // 00000001 = topic count: "1"
        // 0005 = topic name length: 5 bytes
        // 7465737436 = "test6"
        // 00000001 = partition count: "1"
        // 00000002 = partition id: "2"
        // 0000000000000002 = partition offset: "2"
        // 0000 = metadata length: "0"
        // 0003 = error code = 3
        $this->setData(Decoder::Khex2bin('000000230000000000000001000574657374360000000100000002000000000000000200000003'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        $actual  = $decoder->fetchOffsetResponse();

        $expect = array(
            'test6' => array(
                2 => array(
                    'offset'   => 2,
                    'metadata' => '',
                    'errCode'  => 3,
                ),
            ),
        );
        $this->assertEquals($expect, $actual);
    }

    // }}}
    //{{{ public function testFetchOffsetResponseErrorCode15()

    public function testFetchOffsetResponseErrorCode15()
    {
        // 00000023  = data length = 35 bytes
        // 00000000 = ??
        // 00000001 = topic count: "1"
        // 0005 = topic name length: 5 bytes
        // 7465737436 = "test6"
        // 00000001 = partition count: "1"
        // 00000002 = partition id: "2"
        // 0000000000000002 = partition offset: "2"
        // 0000 = metadata length: "0"
        // 000F = error code = 15
        $this->setData(Decoder::Khex2bin('00000023000000000000000100057465737436000000010000000200000000000000020000000F'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        $actual  = $decoder->fetchOffsetResponse();

        $expect = array(
            'test6' => array(
                2 => array(
                    'offset'   => 2,
                    'metadata' => '',
                    'errCode'  => 15,
                ),
            ),
        );
        $this->assertEquals($expect, $actual);
    }

    // }}}
    //{{{ public function testFetchOffsetResponseUnexpectedErrorCode()

    public function testFetchOffsetResponseUnexpectedErrorCode()
    {
        // 00000023  = data length = 35 bytes
        // 00000000 = ??
        // 00000001 = topic count: "1"
        // 0005 = topic name length: 5 bytes
        // 7465737436 = "test6"
        // 00000001 = partition count: "1"
        // 00000002 = partition id: "2"
        // 0000000000000002 = partition offset: "2"
        // 0000 = metadata length: "0"
        // FFFF = error code = -1
        $this->setData(Decoder::Khex2bin('00000023000000000000000100057465737436000000010000000200000000000000020000FFFF'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        $actual  = $decoder->fetchOffsetResponse();

        $expect = array(
            'test6' => array(
                2 => array(
                    'offset'   => 2,
                    'metadata' => '',
                    'errCode'  => -1,
                ),
            ),
        );
        $this->assertEquals($expect, $actual);
    }

    // }}}
    //{{{ public function testFetchOffsetResponseNotData()

    /**
     * testFetchOffsetResponseNotData
     *
     * @access public
     * @return void
     */
    public function testFetchOffsetResponseNotData()
    {
        $this->setData(Decoder::Khex2bin('00000000'));
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
        try {
            $decoder->fetchOffsetResponse();
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('fetch offset response invalid.', $e->getMessage());
        }
    }

    // }}}
    // {{{ public function testGetError()

    /**
     * testGetError
     *
     * @access public
     * @return void
     */
    public function testGetError()
    {
        $this->assertEquals('Unknown error', Decoder::getError(36));
    }

    // }}}
    // }}}
}
