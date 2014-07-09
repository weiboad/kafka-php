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

use \KafkaMock\Protocol\Encoder;

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

class EncoderTest extends \PHPUnit_Framework_TestCase
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
        if (is_null($this->stream)) {
            $this->stream = \Kafka\Socket::createFromStream(fopen('php://temp', 'w+b'));
        }
        $this->stream->rewind();
    }

    // }}}
    // {{{ public function getData()

    /**
     * getData
     *
     * @access public
     * @return void
     */
    public function getData($len)
    {
        $this->stream->rewind();
        $data = $this->stream->read($len, true);
        return bin2hex($data);
    }

    // }}}
    // {{{ public function testEncodeMessage()

    /**
     * testEncodeMessage
     *
     * @access public
     * @return void
     */
    public function testEncodeMessage()
    {
        $test = Encoder::encodeMessage('test');
        $this->assertEquals(bin2hex($test), 'dd007e170000000000000000000474657374');
    }

    // }}}
    // {{{ public function testEncodeString()

    /**
     * testEncodeString
     *
     * @access public
     * @return void
     */
    public function testEncodeString()
    {
        $expect = Encoder::encodeString('test', Encoder::PACK_INT32);
        $this->assertEquals(bin2hex($expect), '000000474657374');

        $expect = Encoder::encodeString('test', Encoder::PACK_INT16);
        $this->assertEquals(bin2hex($expect), '000474657374');

        $expect = Encoder::encodeString('test', Encoder::PACK_INT32, Encoder::COMPRESSION_GZIP);
        $this->assertEquals(bin2hex($expect), '000000181f8b08000000000000032b492d2e01000c7e7fd804000000');

        try {
            $expect = Encoder::encodeString('test', Encoder::PACK_INT32, Encoder::COMPRESSION_SNAPPY);
        } catch (\Kafka\Exception\NotSupported $e) {
            $this->assertSame('SNAPPY compression not yet implemented', $e->getMessage());
        }

        try {
            $expect = Encoder::encodeString('test', Encoder::PACK_INT32, 4);
        } catch (\Kafka\Exception\NotSupported $e) {
            $this->assertSame('Unknown compression flag: 4', $e->getMessage());
        }
    }

    // }}}
    // {{{ public function testEncodeArray()

    /**
     * testEncodeArray
     *
     * @access public
     * @return void
     */
    public function testEncodeArray()
    {
        $array = array('test1', 'test2', 'test3');
        try {
            Encoder::encodeArray($array, 'notCallable');
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('Encode array failed, given function is not callable.', $e->getMessage());
        }

        $actual = Encoder::encodeArray($array, array('\KafkaMock\Protocol\Encoder', 'encodeString'), Encoder::PACK_INT32);
        $this->assertEquals('00000003000000057465737431000000057465737432000000057465737433', bin2hex($actual));
    }

    // }}}
    // {{{ public function testEncodeMessageSet()

    /**
     * testEncodeMessageSet
     *
     * @access public
     * @return void
     */
    public function testEncodeMessageSet()
    {
        $message = 'test';
        $actual = Encoder::encodeMessageSet($message);
        $this->assertEquals('000000000000000000000012dd007e170000000000000000000474657374', bin2hex($actual));

        $array = array(
            'test',
            'test1',
        );
        $actual = Encoder::encodeMessageSet($array);
        $this->assertEquals('000000000000000000000012dd007e170000000000000000000474657374000000000000000000000013cb8eb9ab000000000000000000057465737431', bin2hex($actual));
    }

    // }}}
    // {{{ public function testRequestHeader()

    /**
     * testRequestHeader
     *
     * @access public
     * @return void
     */
    public function testRequestHeader()
    {
        $actual = Encoder::requestHeader('kafka-php', '0', 0);
        $this->assertEquals('000000000000000000096b61666b612d706870', bin2hex($actual));
    }

    // }}}
    // {{{ public function testEncodeProcudePartion()

    /**
     * testEncodeProcudePartion
     *
     * @access public
     * @return void
     */
    public function testEncodeProcudePartion()
    {
        $data = array(
            'partition_id' => 1,
            'messages' => array(
                'test',
                'test1',
            ),
        );
        $actual = Encoder::encodeProcudePartion($data, Encoder::COMPRESSION_NONE);
        $this->assertEquals('000000010000003d000000000000000000000012dd007e170000000000000000000474657374000000000000000000000013cb8eb9ab000000000000000000057465737431', bin2hex($actual));

        $data = array(
            'messages' => array(
                'test',
            ),
        );
        try {
            Encoder::encodeProcudePartion($data, Encoder::COMPRESSION_NONE);
        } catch(\Kafka\Exception\Protocol $e) {
            $this->assertSame('given produce data invalid. `partition_id` is undefined.', $e->getMessage());
        }

        $data = array(
            'partition_id' => 0,
        );
        try {
            Encoder::encodeProcudePartion($data, Encoder::COMPRESSION_NONE);
        } catch(\Kafka\Exception\Protocol $e) {
            $this->assertSame('given produce data invalid. `messages` is undefined.', $e->getMessage());
        }
    }

    // }}}
    // {{{ public function testEncodeProcudeTopic()

    /**
     * testEncodeProcudeTopic
     *
     * @access public
     * @return void
     */
    public function testEncodeProcudeTopic()
    {
        $data = array(
            'topic_name' => 'test',
            'partitions' => array(
                array(
                    'partition_id' => 0,
                    'messages'     => array(
                        'test',
                        'test1',
                    ),
                ),
            ),
        );
        $actual = Encoder::encodeProcudeTopic($data, Encoder::COMPRESSION_NONE);
        $this->assertEquals('00047465737400000001000000000000003d000000000000000000000012dd007e170000000000000000000474657374000000000000000000000013cb8eb9ab000000000000000000057465737431', bin2hex($actual));

        $data = array(
            'partitions' => array(
                array(
                    'partition_id' => 0,
                    'messages'     => array(
                        'test',
                        'test1',
                    ),
                ),
            ),
        );
        try {
            Encoder::encodeProcudeTopic($data, Encoder::COMPRESSION_NONE);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given produce data invalid. `topic_name` is undefined.', $e->getMessage());
        }

        $data = array(
            'topic_name' => 'test',
        );
        try {
            Encoder::encodeProcudeTopic($data, Encoder::COMPRESSION_NONE);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given produce data invalid. `partitions` is undefined.', $e->getMessage());
        }
    }

    // }}}
    // {{{ public function testEncodeFetchPartion()

    /**
     * testEncodeFetchPartion
     *
     * @access public
     * @return void
     */
    public function testEncodeFetchPartion()
    {
        $data = array(
            'partition_id' => 1,
            'offset' => 2,
        );
        $actual = Encoder::encodeFetchPartion($data);
        $this->assertEquals('00000001000000000000000206400000', bin2hex($actual));

        $data = array(
        );
        try {
            Encoder::encodeFetchPartion($data);
        } catch(\Kafka\Exception\Protocol $e) {
            $this->assertSame('given fetch data invalid. `partition_id` is undefined.', $e->getMessage());
        }

        $data = array(
            'partition_id' => 0,
        );
        $actual = Encoder::encodeFetchPartion($data);
        $this->assertEquals('00000000000000000000000006400000', bin2hex($actual));
    }

    // }}}
    // {{{ public function testEncodeProcudeTopic()

    /**
     * testEncodeProcudeTopic
     *
     * @access public
     * @return void
     */
    public function testEncodeFetchTopic()
    {
        $data = array(
            'topic_name' => 'test',
            'partitions' => array(
                array(
                    'partition_id' => 0,
                ),
            ),
        );
        $actual = Encoder::encodeFetchTopic($data);
        $this->assertEquals('0004746573740000000100000000000000000000000006400000', bin2hex($actual));

        $data = array(
            'partitions' => array(
                array(
                    'partition_id' => 0,
                ),
            ),
        );
        try {
            Encoder::encodeFetchTopic($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given fetch data invalid. `topic_name` is undefined.', $e->getMessage());
        }

        $data = array(
            'topic_name' => 'test',
        );
        try {
            Encoder::encodeFetchTopic($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given fetch data invalid. `partitions` is undefined.', $e->getMessage());
        }
    }

    // }}}
    // {{{ public function testEncodeOffsetPartion()

    /**
     * testEncodeOffsetPartion
     *
     * @access public
     * @return void
     */
    public function testEncodeOffsetPartion()
    {
        $data = array(
            'partition_id' => 1,
        );
        $actual = Encoder::encodeOffsetPartion($data);
        $this->assertEquals('00000001ffffffffffffffff000186a0', bin2hex($actual));

        $data = array(
        );
        try {
            Encoder::encodeOffsetPartion($data);
        } catch(\Kafka\Exception\Protocol $e) {
            $this->assertSame('given offset data invalid. `partition_id` is undefined.', $e->getMessage());
        }
    }

    // }}}
    // {{{ public function testEncodeOffsetTopic()

    /**
     * testEncodeOffsetTopic
     *
     * @access public
     * @return void
     */
    public function testEncodeOffsetTopic()
    {
        $data = array(
            'topic_name' => 'test',
            'partitions' => array(
                array(
                    'partition_id' => 0,
                ),
            ),
        );
        $actual = Encoder::encodeOffsetTopic($data);
        $this->assertEquals('0004746573740000000100000000ffffffffffffffff000186a0', bin2hex($actual));

        $data = array(
            'partitions' => array(
                array(
                    'partition_id' => 0,
                ),
            ),
        );
        try {
            Encoder::encodeOffsetTopic($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given offset data invalid. `topic_name` is undefined.', $e->getMessage());
        }

        $data = array(
            'topic_name' => 'test',
        );
        try {
            Encoder::encodeOffsetTopic($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given offset data invalid. `partitions` is undefined.', $e->getMessage());
        }
    }

    // }}}
    // {{{ public function testEncodeCommitOffsetPartion()

    /**
     * testEncodeCommitOffsetPartion
     *
     * @access public
     * @return void
     */
    public function testEncodeCommitOffsetPartion()
    {
        $data = array(
            'partition_id' => 1,
            'offset' => 2,
        );
        $actual = Encoder::encodeCommitOffsetPartion($data);
        $this->assertEquals('000000010000000000000002ffffffffffffffff00016d', bin2hex($actual));

        $data = array(
            'partition_id' => 1,
        );
        try {
            Encoder::encodeCommitOffsetPartion($data);
        } catch(\Kafka\Exception\Protocol $e) {
            $this->assertSame('given commit offset data invalid. `offset` is undefined.', $e->getMessage());
        }

        $data = array(
            'offset' => 2,
        );
        try {
            Encoder::encodeCommitOffsetPartion($data);
        } catch(\Kafka\Exception\Protocol $e) {
            $this->assertSame('given commit offset data invalid. `partition_id` is undefined.', $e->getMessage());
        }
    }

    // }}}
    // {{{ public function testEncodeCommitOffset()

    /**
     * testEncodeCommitOffset
     *
     * @access public
     * @return void
     */
    public function testEncodeCommitOffset()
    {
        $data = array(
            'topic_name' => 'test',
            'partitions' => array(
                array(
                    'partition_id' => 0,
                    'offset' => 2,
                ),
            ),
        );
        $actual = Encoder::encodeCommitOffset($data);
        $this->assertEquals('00047465737400000001000000000000000000000002ffffffffffffffff00016d', bin2hex($actual));

        $data = array(
            'partitions' => array(
                array(
                    'partition_id' => 0,
                ),
            ),
        );
        try {
            Encoder::encodeCommitOffset($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given commit offset data invalid. `topic_name` is undefined.', $e->getMessage());
        }

        $data = array(
            'topic_name' => 'test',
        );
        try {
            Encoder::encodeCommitOffset($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given commit offset data invalid. `partitions` is undefined.', $e->getMessage());
        }
    }

    // }}}
    // {{{ public function testEncodeFetchOffsetPartion()

    /**
     * testEncodeFetchOffsetPartion
     *
     * @access public
     * @return void
     */
    public function testEncodeFetchOffsetPartion()
    {
        $data = array(
            'partition_id' => 1,
        );
        $actual = Encoder::encodeFetchOffsetPartion($data);
        $this->assertEquals('00000001', bin2hex($actual));

        $data = array(
        );
        try {
            Encoder::encodeFetchOffsetPartion($data);
        } catch(\Kafka\Exception\Protocol $e) {
            $this->assertSame('given fetch offset data invalid. `partition_id` is undefined.', $e->getMessage());
        }
    }

    // }}}
    // {{{ public function testEncodeFetchOffset()

    /**
     * testEncodeFetchOffset
     *
     * @access public
     * @return void
     */
    public function testEncodeFetchOffset()
    {
        $data = array(
            'topic_name' => 'test',
            'partitions' => array(
                array(
                    'partition_id' => 0,
                    'offset' => 2,
                ),
            ),
        );
        $actual = Encoder::encodeFetchOffset($data);
        $this->assertEquals('0004746573740000000100000000', bin2hex($actual));

        $data = array(
            'partitions' => array(
                array(
                    'partition_id' => 0,
                ),
            ),
        );
        try {
            Encoder::encodeFetchOffset($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given fetch offset data invalid. `topic_name` is undefined.', $e->getMessage());
        }

        $data = array(
            'topic_name' => 'test',
        );
        try {
            Encoder::encodeFetchOffset($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given fetch offset data invalid. `partitions` is undefined.', $e->getMessage());
        }
    }

    // }}}
    // {{{ public function testProduceRequest()

    /**
     * testProduceRequest
     *
     * @access public
     * @return void
     */
    public function testProduceRequest()
    {
        $encoder = new \Kafka\Protocol\Encoder($this->stream);

        $data = array();
        try {
            $encoder->produceRequest($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given procude data invalid. `data` is undefined.', $e->getMessage());
        }

        $data = array(
            'data' => array(
                array(
                    'topic_name' => 'debug',
                    'partitions' => array(
                        array(
                            'partition_id' => 1,
                            'messages' => array(
                                'test1',
                            ),
                        ),
                    ),
                ),
            ),
        );
        $len = $encoder->produceRequest($data);
        $this->assertEquals('0000004f000000000000000000096b61666b612d706870000000000064000000010005646562756700000001000000010000001f000000000000000000000013cb8eb9ab000000000000000000057465737431', $this->getData($len));
    }

    // }}}
    // {{{ public function testMetadataRequest()

    /**
     * testMetadataRequest
     *
     * @access public
     * @return void
     */
    public function testMetadataRequest()
    {
        $encoder = new \Kafka\Protocol\Encoder($this->stream);

        $data = array();
        try {
            $encoder->metadataRequest(null);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('request metadata topic array have invalid value. ', $e->getMessage());
        }

        $data = array(
            'test',
            'test1',
        );
        $len = $encoder->metadataRequest($data);
        $this->assertEquals('00000024000300000000000000096b61666b612d7068700000000200047465737400057465737431', $this->getData($len));

        $this->stream->rewind();
        $data = 'test1';
        $len = $encoder->metadataRequest($data);
        $this->assertEquals('0000001e000300000000000000096b61666b612d7068700000000100057465737431', $this->getData($len));
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
        $encoder = new \Kafka\Protocol\Encoder($this->stream);

        $data = array();
        try {
            $encoder->fetchRequest($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given fetch kafka data invalid. `data` is undefined.', $e->getMessage());
        }

        $data = array(
            'data' => array(
                array(
                    'topic_name' => 'debug',
                    'partitions' => array(
                        array(
                            'partition_id' => 1,
                        ),
                    ),
                ),
            ),
        );
        $len = $encoder->fetchRequest($data);
        $this->assertEquals('0000003e000100000000000000096b61666b612d706870ffffffff000000640001000000000001000564656275670000000100000001000000000000000006400000', $this->getData($len));
    }

    // }}}
    // {{{ public function testOffsetRequest()

    /**
     * testOffsetRequest
     *
     * @access public
     * @return void
     */
    public function testOffsetRequest()
    {
        $encoder = new \Kafka\Protocol\Encoder($this->stream);

        $data = array();
        try {
            $encoder->offsetRequest($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given offset data invalid. `data` is undefined.', $e->getMessage());
        }

        $data = array(
            'data' => array(
                array(
                    'topic_name' => 'debug',
                    'partitions' => array(
                        array(
                            'partition_id' => 1,
                        ),
                    ),
                ),
            ),
        );
        $len = $encoder->offsetRequest($data);
        $this->assertEquals('00000036000200000000000000096b61666b612d706870ffffffff00000001000564656275670000000100000001ffffffffffffffff000186a0', $this->getData($len));
    }

    // }}}
    // {{{ public function testCommitOffsetRequest()

    /**
     * testCommitOffsetRequest
     *
     * @access public
     * @return void
     */
    public function testCommitOffsetRequest()
    {
        $encoder = new \Kafka\Protocol\Encoder($this->stream);

        $data = array();
        try {
            $encoder->commitOffsetRequest($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given commit offset data invalid. `data` is undefined.', $e->getMessage());
        }

        $data = array(
            'data' => array(
                array(
                    'topic_name' => 'debug',
                    'partitions' => array(),
                ),
            ),
        );
        try {
            $encoder->commitOffsetRequest($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given commit offset data invalid. `group_id` is undefined.', $e->getMessage());
        }

        $data = array(
            'group_id' => 'testgroup',
            'data' => array(
                array(
                    'topic_name' => 'debug',
                    'partitions' => array(
                        array(
                            'partition_id' => 1,
                            'offset' => 2,
                        ),
                    ),
                ),
            ),
        );
        $len = $encoder->commitOffsetRequest($data);
        $this->assertEquals('00000044000800000000000000096b61666b612d70687000097465737467726f7570000000010005646562756700000001000000010000000000000002ffffffffffffffff00016d', $this->getData($len));
    }

    // }}}
    // {{{ public function testFetchOffsetRequest()

    /**
     * testFetchOffsetRequest
     *
     * @access public
     * @return void
     */
    public function testFetchOffsetRequest()
    {
        $encoder = new \Kafka\Protocol\Encoder($this->stream);

        $data = array();
        try {
            $encoder->fetchOffsetRequest($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given fetch offset data invalid. `data` is undefined.', $e->getMessage());
        }

        $data = array(
            'data' => array(
                array(
                    'topic_name' => 'debug',
                    'partitions' => array(),
                ),
            ),
        );
        try {
            $encoder->fetchOffsetRequest($data);
        } catch (\Kafka\Exception\Protocol $e) {
            $this->assertSame('given fetch offset data invalid. `group_id` is undefined.', $e->getMessage());
        }

        $data = array(
            'group_id' => 'testgroup',
            'data' => array(
                array(
                    'topic_name' => 'debug',
                    'partitions' => array(
                        array(
                            'partition_id' => 1,
                        ),
                    ),
                ),
            ),
        );
        $len = $encoder->fetchOffsetRequest($data);
        $this->assertEquals('00000031000900000000000000096b61666b612d70687000097465737467726f757000000001000564656275670000000100000001', $this->getData($len));
    }

    // }}}
    // }}}
}
