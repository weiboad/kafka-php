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

class OffsetTest extends \PHPUnit_Framework_TestCase
{
    // {{{ consts
    // }}}
    // {{{ members

    /**
     * offset object
     *
     * @var mixed
     * @access protected
     */
    protected $offset = null;

    /**
     * offset object v0.10
     *
     * @var mixed
     * @access protected
     */
    protected $offset10 = null;

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
        if (is_null($this->offset)) {
            $this->offset = new \Kafka\Protocol\Offset('0.9.0.1');
        }
        if (is_null($this->offset10)) {
            $this->offset10 = new \Kafka\Protocol\Offset('0.10.1.0');
        }
    }

    // }}}
    // {{{ public function testEncode()

    /**
     * testEncode
     *
     * @access public
     * @return void
     */
    public function testEncode()
    {
        $data = array(
            'data' => array(
                array(
                    'topic_name' => 'test',
                    'partitions' => array(
                        array(
                            'partition_id' => 0,
                            'offset' => 100,
                        ),
                    ),
                ),
            ),
        );

        $test = $this->offset->encode($data);
        $this->assertEquals(\bin2hex($test), '00000035000200000000000200096b61666b612d706870ffffffff000000010004746573740000000100000000ffffffffffffffff000186a0');
        $test = $this->offset10->encode($data);
        $this->assertEquals(\bin2hex($test), '00000035000200000000000200096b61666b612d706870ffffffff000000010004746573740000000100000000ffffffffffffffff000186a0');
    }

    // }}}
    // {{{ public function testEncodeNoData()

    /**
     * testEncodeNoData
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given offset data invalid. `data` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoData()
    {
        $data = array(
        );

        $test = $this->offset->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoTopicName()

    /**
     * testEncodeNoTopicName
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given offset data invalid. `topic_name` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoTopicName()
    {
        $data = array(
            'data' => array(
                array(
                ),
            ),
        );

        $test = $this->offset->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoPartitions()

    /**
     * testEncodeNoPartitions
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given offset data invalid. `partitions` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoPartitions()
    {
        $data = array(
            'data' => array(
                array(
                    'topic_name' => 'test',
                ),
            ),
        );

        $test = $this->offset->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoPartitionId()

    /**
     * testEncodeNoPartitionId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given offset data invalid. `partition_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoPartitionId()
    {
        $data = array(
            'data' => array(
                array(
                    'topic_name' => 'test',
                    'partitions' => array(
                        array(
                        ),
                    ),
                ),
            ),
        );

        $test = $this->offset->encode($data);
    }

    // }}}
    // {{{ public function testDecode()

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        $data = '000000010004746573740000000100000000000000000001000000000000002a';
        $test = $this->offset->decode(\hex2bin($data));
        $result = '[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"timestamp":0,"offsets":[42]}]}]';
        $this->assertEquals(json_encode($test), $result);
    }

    // }}}
    // }}}
}
