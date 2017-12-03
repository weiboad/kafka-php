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

class CommitOffsetTest extends \PHPUnit\Framework\TestCase
{
    // {{{ consts
    // }}}
    // {{{ members

    /**
     * commit object
     *
     * @var mixed
     * @access protected
     */
    protected $commit = null;

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
        if (is_null($this->commit)) {
            $this->commit = new \Kafka\Protocol\CommitOffset('0.9.0.1');
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
            'generation_id' => 2,
            'member_id' => 'kafka-php-c7e3d40a-57d8-4220-9523-cebfce9a0685',
            'retention_time' => 36000,
            'data' => array(
                array(
                    'topic_name' => 'test',
                    'partitions' => array(
                        array(
                            'partition' => 0,
                            'offset' => 45,
                            'metadata' => '',
                        )
                    )
                )
            ),
        );
        $test = $this->commit->encode($data);
        $this->assertEquals(\bin2hex($test), '00000071000800020000000800096b61666b612d70687000047465737400000002002e6b61666b612d7068702d63376533643430612d353764382d343232302d393532332d6365626663653961303638350000000000008ca0000000010004746573740000000100000000000000000000002d0000');
    }

    // }}}
    // {{{ public function testEncodeDefault()

    /**
     * testEncodeDefault
     *
     * @access public
     * @return void
     */
    public function testEncodeDefault()
    {
        $data = array(
            'group_id' => 'test',
            'data' => array(
                array(
                    'topic_name' => 'test',
                    'partitions' => array(
                        array(
                            'partition' => 0,
                            'offset' => 45,
                        )
                    )
                )
            ),
        );
        $test = $this->commit->encode($data);
        $this->assertEquals(\bin2hex($test), '00000043000800020000000800096b61666b612d706870000474657374ffffffff0000ffffffffffffffff000000010004746573740000000100000000000000000000002d0000');
    }

    // }}}
    // {{{ public function testEncodeNoData()

    /**
     * testEncodeNoData
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given commit data invalid. `data` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoData()
    {
        $data = array(
            'group_id' => 'test'
        );

        $test = $this->commit->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoGroupId()

    /**
     * testEncodeNoGroupId
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given commit offset data invalid. `group_id` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoGroupId()
    {
        $data = array(
            'data' => array()
        );

        $test = $this->commit->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoTopicName()

    /**
     * testEncodeNoTopicName
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given commit offset data invalid. `topic_name` is undefined.
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

        $test = $this->commit->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoPartitions()

    /**
     * testEncodeNoPartitions
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given commit offset data invalid. `partitions` is undefined.
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
        $test = $this->commit->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoPartition()

    /**
     * testEncodeNoPartition
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given commit offset data invalid. `partition` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoPartition()
    {
        $data = array(
            'group_id' => 'test',
            'data' => array(
                array(
                    'topic_name' => 'test',
                    'partitions' => array(
                        array()
                    )
                )
            ),
        );
        $test = $this->commit->encode($data);
    }

    // }}}
    // {{{ public function testEncodeNoOffset()

    /**
     * testEncodeNoOffset
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage given commit offset data invalid. `offset` is undefined.
     * @access public
     * @return void
     */
    public function testEncodeNoOffset()
    {
        $data = array(
            'group_id' => 'test',
            'data' => array(
                array(
                    'topic_name' => 'test',
                    'partitions' => array(
                        array(
                            'partition' => 0
                        )
                    )
                )
            ),
        );
        $test = $this->commit->encode($data);
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
        $data = '0000000100047465737400000001000000000000';
        $test = $this->commit->decode(\hex2bin($data));
        $result = '[{"topicName":"test","partitions":[{"partition":0,"errorCode":0}]}]';
        $this->assertEquals(json_encode($test), $result);
    }

    // }}}
    // }}}
}
