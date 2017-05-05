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

namespace KafkaTest;

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

class ProducerConfigTest extends \PHPUnit_Framework_TestCase
{
    // {{{ consts
    // }}}
    // {{{ members
    // }}}
    // {{{ functions
    // {{{ public function setDown()

    /**
     * setDown
     *
     * @access public
     * @return void
     */
    public function tearDown()
    {
        \Kafka\ProducerConfig::getInstance()->clear();
    }

    // }}}
    // {{{ public function testSetRequestTimeout()

    /**
     * testSetRequestTimeout
     *
     * @access public
     * @return void
     */
    public function testSetRequestTimeout()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setRequestTimeout(1011);
        $this->assertEquals($config->getRequestTimeout(), 1011);
    }

    // }}}
    // {{{ public function testSetRequestTimeoutValid()

    /**
     * testSetRequestTimeoutValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set request timeout value is invalid, must set it 1 .. 900000
     * @access public
     * @return void
     */
    public function testSetRequestTimeoutValid()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setRequestTimeout('-1');
    }

    // }}}
    // {{{ public function testSetProduceInterval()

    /**
     * testSetProduceInterval
     *
     * @access public
     * @return void
     */
    public function testSetProduceInterval()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setProduceInterval(1011);
        $this->assertEquals($config->getProduceInterval(), 1011);
    }

    // }}}
    // {{{ public function testSetProduceIntervalValid()

    /**
     * testSetProduceIntervalValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set produce interval timeout value is invalid, must set it 1 .. 900000
     * @access public
     * @return void
     */
    public function testSetProduceIntervalValid()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setProduceInterval('-1');
    }

    // }}}
    // {{{ public function testSetTimeout()

    /**
     * testSetTimeout
     *
     * @access public
     * @return void
     */
    public function testSetTimeout()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setTimeout(1011);
        $this->assertEquals($config->getTimeout(), 1011);
    }

    // }}}
    // {{{ public function testSetTimeoutValid()

    /**
     * testSetTimeoutValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set timeout value is invalid, must set it 1 .. 900000
     * @access public
     * @return void
     */
    public function testSetTimeoutValid()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setTimeout('-1');
    }

    // }}}
    // {{{ public function testSetRequiredAck()

    /**
     * testSetRequiredAck
     *
     * @access public
     * @return void
     */
    public function testSetRequiredAck()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setRequiredAck(1);
        $this->assertEquals($config->getRequiredAck(), 1);
    }

    // }}}
    // {{{ public function testSetRequiredAckValid()

    /**
     * testSetRequiredAckValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set required ack value is invalid, must set it -1 .. 1000
     * @access public
     * @return void
     */
    public function testSetRequiredAckValid()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setRequiredAck('-2');
    }

    // }}}
    // {{{ public function testSetIsAsyn()

    /**
     * testSetIsAsyn
     *
     * @access public
     * @return void
     */
    public function testSetIsAsyn()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setIsAsyn(true);
        $this->assertTrue($config->getIsAsyn());
    }

    // }}}
    // {{{ public function testSetIsAsynValid()

    /**
     * testSetIsAsynValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set isAsyn value is invalid, must set it bool value
     * @access public
     * @return void
     */
    public function testSetIsAsynValid()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setIsAsyn('-2');
    }

    // }}}
    // }}}
}
