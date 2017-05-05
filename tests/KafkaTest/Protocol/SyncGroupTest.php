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

class SyncGroupTest extends \PHPUnit_Framework_TestCase
{
    // {{{ consts
    // }}}
    // {{{ members

    /**
     * sync object
     *
     * @var mixed
     * @access protected
     */
    protected $sync = null;

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
        if (is_null($this->sync)) {
            $this->sync = new \Kafka\Protocol\SyncGroup('0.9.0.1');
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
        $data = json_decode('{"group_id":"test","generation_id":1,"member_id":"kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c","data":[{"version":0,"member_id":"kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c","assignments":[{"topic_name":"test","partitions":[0]}]}]}', true);

        $test = $this->sync->encode($data);
        $this->assertEquals(\bin2hex($test), '0000009d000e00000000000e00096b61666b612d70687000047465737400000001002e6b61666b612d7068702d62643564356262322d326131662d343364342d623833312d62313531306438316163356300000001002e6b61666b612d7068702d62643564356262322d326131662d343364342d623833312d62313531306438316163356300000018000000000001000474657374000000010000000000000000');
    }

    // }}}
    // {{{ public function testEncodeNoGroupId()

    /**
     * testEncodeNoGroupId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given sync group data invalid. `group_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGroupId()
    {
        $data = array();

        $test = $this->sync->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoGenerationId()

    /**
     * testEncodeNoGenerationId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given sync group data invalid. `generation_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGenerationId()
    {
        $data = array(
            'group_id' => 'test',
        );

        $test = $this->sync->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoMemberId()

    /**
     * testEncodeNoMemberId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given sync group data invalid. `member_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoMemberId()
    {
        $data = array(
            'group_id' => 'test',
            'generation_id' => '1',
        );

        $test = $this->sync->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoData()

    /**
     * testEncodeNoData
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given sync group data invalid. `data` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoData()
    {
        $data = array(
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c'
        );

        $test = $this->sync->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoVersion()

    /**
     * testEncodeNoVersion
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `version` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoVersion()
    {
        $data = array(
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => array(
                array(
                
                ),
            ),
        );

        $test = $this->sync->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoDataMemberId()

    /**
     * testEncodeNoDataMemberId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `member_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoDataMemberId()
    {
        $data = array(
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => array(
                array(
                    'version' => 0
                ),
            ),
        );

        $test = $this->sync->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoDataAssignments()

    /**
     * testEncodeNoDataAssignments
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `assignments` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoDataAssignments()
    {
        $data = array(
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => array(
                array(
                    'version' => 0 ,
                    'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
                ),
            ),
        );

        $test = $this->sync->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoTopicName()

    /**
     * testEncodeNoTopicName
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `topic_name` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoTopicName()
    {
        $data = array(
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => array(
                array(
                    'version' => 0 ,
                    'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
                    'assignments' => array(
                        array(),
                    )
                ),
            ),
        );

        $test = $this->sync->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoPartitions()

    /**
     * testEncodeNoPartitions
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given data invalid. `partitions` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoPartitions()
    {
        $data = array(
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => array(
                array(
                    'version' => 0 ,
                    'member_id' => 'kafka-php-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
                    'assignments' => array(
                        array(
                            'topic_name' => 'test',
                        ),
                    )
                ),
            ),
        );

        $test = $this->sync->encode($data);
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
        $data = '000000000018000000000001000474657374000000010000000000000000';
        $test = $this->sync->decode(\hex2bin($data));
        $result = '{"errorCode":0,"partitionAssignments":[{"topicName":"test","partitions":[0]}],"version":0,"userData":""}';
        $this->assertEquals(json_encode($test), $result);
        $test = $this->sync->decode(\hex2bin('000000000000'));
        $result = '{"errorCode":0}';
        $this->assertEquals(json_encode($test), $result);
    }

    // }}}
    // }}}
}
