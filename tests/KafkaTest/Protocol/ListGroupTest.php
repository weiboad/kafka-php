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

class ListGroupTest extends \PHPUnit_Framework_TestCase
{
    // {{{ consts
    // }}}
    // {{{ members

    /**
     * list object
     *
     * @var mixed
     * @access protected
     */
    protected $list = null;

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
        if (is_null($this->list)) {
            $this->list = new \Kafka\Protocol\ListGroup('0.9.0.1');
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
        );

        $test = $this->list->encode($data);
        $this->assertEquals(\bin2hex($test), '00000013001000000000001000096b61666b612d706870');
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
        $test = $this->list->decode(\hex2bin('0000000000010004746573740008636f6e73756d6572'));
        $result = '{"errorCode":0,"groups":[{"groupId":"test","protocolType":"consumer"}]}';
        $this->assertEquals(json_encode($test), $result);
    }

    // }}}
    // }}}
}
