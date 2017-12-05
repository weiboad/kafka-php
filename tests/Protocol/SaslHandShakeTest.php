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

class SaslHandShakeTest extends \PHPUnit\Framework\TestCase
{
    // {{{ consts
    // }}}
    // {{{ members

    /**
     * sasl object
     *
     * @var mixed
     * @access protected
     */
    protected $sasl = null;

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
        if (is_null($this->sasl)) {
            $this->sasl = new \Kafka\Protocol\SaslHandShake('0.10.0.0');
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
        $data = 'PLAIN';
        $test = $this->sasl->encode($data);
        $this->assertEquals(\bin2hex($test), '0000001a001100000000001100096b61666b612d7068700005504c41494e');
    }

    // }}}
    // {{{ public function testEncodeIsNotString()

    /**
     * testEncodeIsNotString
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessage request sasl hand shake mechanism invalid value.
     * @access public
     * @return void
     */
    public function testEncodeIsNotString()
    {
        $data = [
        ];

        $test = $this->sasl->encode($data);
    }

    // }}}
    // {{{ public function testEncodeIsNotAllow()

    /**
     * testEncodeIsNotAllow
     *
     * @expectedException \Kafka\Exception\Protocol
     * @expectedExceptionMessageRegExp /request sasl hand shake mechanism invalid value, must in \w+/
     * @access public
     * @return void
     */
    public function testEncodeIsNotAllow()
    {
        $data = 'NOTALLOW';

        $test = $this->sasl->encode($data);
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
        $data   = '0022000000010006475353415049';
        $test   = $this->sasl->decode(\hex2bin($data));
        $result = '{"mechanisms":["GSSAPI"],"errorCode":34}';
        $this->assertEquals(json_encode($test), $result);
    }

    // }}}
    // }}}
}
