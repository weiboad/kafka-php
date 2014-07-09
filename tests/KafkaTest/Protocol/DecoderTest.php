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

use \KafkaMock\Protocol\Decoder;

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
    // {{{ public function getData()

    /**
     * getData
     *
     * @access public
     * @return void
     */
    public function setData($data)
    {
        $len = $this->stream->write($data, true);
        $this->stream->rewind();
        return $len;
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
        $decoder = new \Kafka\Protocol\Decoder($this->stream);
    }

    // }}}
    // }}}
}
