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
    // }}}
    // {{{ functions
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
    // }}}
}
