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

class FetchOffsetTest extends \PHPUnit\Framework\TestCase
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
            $this->offset = new \Kafka\Protocol\FetchOffset('0.9.0.1');
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
            'group_id' => 'test',
            'data' => array(
                array(
                    'topic_name' => 'test',
                    'partitions' => array(0)
                )
            ),
        );

        $test = $this->offset->encode($data);
        $this->assertEquals(\bin2hex($test), '0000002b000900010000000900096b61666b612d706870000474657374000000010004746573740000000100000000');
    }

    // }}}
    // {{{ public function testEncodeNoData()

    /**
     * testEncodeNoData
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch offset data invalid. `data` is undefined.
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
    // {{{ public function testEncodeNoGroupId()

    /**
     * testEncodeNoGroupId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch offset data invalid. `group_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGroupId()
    {
        $data = array(
            'data' => array()
        );

        $test = $this->offset->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoTopicName()

    /**
     * testEncodeNoTopicName
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given fetch offset data invalid. `topic_name` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoTopicName()
    {
        $data = array(
            'group_id' => 'test',
            'data' => array(
                array(
                )
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
     * @expectedExceptionMessage given fetch offset data invalid. `partitions` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoPartitions()
    {
        $data = array(
            'group_id' => 'test',
            'data' => array(
                array(
                    'topic_name' => 'test',
                )
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
        $data = '000000010004746573740000000100000000ffffffffffffffff00000000';
        $test = $this->offset->decode(\hex2bin($data));
        $result = '[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"metadata":"","offset":-1}]}]';
        $this->assertEquals(json_encode($test), $result);
    }

    // }}}
    // }}}
}
