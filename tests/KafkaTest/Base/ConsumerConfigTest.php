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

class ConsumerConfigTest extends \PHPUnit_Framework_TestCase
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
        \Kafka\ConsumerConfig::getInstance()->clear();
    }

    // }}}
    // {{{ public function testDefaultConfig()

    /**
     * testDefaultConfig
     *
     * @access public
     * @return void
     */
    public function testDefaultConfig()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $this->assertEquals($config->getClientId(), 'kafka-php');
        $this->assertEquals($config->getSessionTimeout(), 30000);
        $this->assertFalse($config->setValidKey('xxx', '222'));
        $config->setValidKey('222');
        $this->assertEquals($config->getValidKey(), '222');
    }

    // }}}
    // {{{ public function testSetClientId()

    /**
     * testSetClientId
     *
     * @access public
     * @return void
     */
    public function testSetClientId()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setClientId('kafka-php1');
        $this->assertEquals($config->getClientId(), 'kafka-php1');
    }

    // }}}
    // {{{ public function testSetClientIdEmpty()

    /**
     * testSetClientIdEmpty
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set clientId value is invalid, must is not empty string.
     * @access public
     * @return void
     */
    public function testSetClientIdEmpty()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setClientId('');
    }

    // }}}
    // {{{ public function testSetBrokerVersion()

    /**
     * testSetBrokerVersion
     *
     * @access public
     * @return void
     */
    public function testSetBrokerVersion()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setBrokerVersion('0.9.0.1');
        $this->assertEquals($config->getBrokerVersion(), '0.9.0.1');
    }

    // }}}
    // {{{ public function testSetBrokerVersionEmpty()

    /**
     * testSetBrokerVersionEmpty
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set broker version value is invalid, must is not empty string and gt 0.8.0.
     * @access public
     * @return void
     */
    public function testSetBrokerVersionEmpty()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setBrokerVersion('');
    }

    // }}}
    // {{{ public function testSetBrokerVersionValid()

    /**
     * testSetBrokerVersionValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set broker version value is invalid, must is not empty string and gt 0.8.0.
     * @access public
     * @return void
     */
    public function testSetBrokerVersionValid()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setBrokerVersion('0.1');
    }

    // }}}
    // {{{ public function testSetMetadataBrokerList()

    /**
     * testSetMetadataBrokerList
     *
     * @access public
     * @return void
     */
    public function testSetMetadataBrokerList()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataBrokerList('127.0.0.1:9192,127.0.0.1:9292');
        $this->assertEquals($config->getMetadataBrokerList(), '127.0.0.1:9192,127.0.0.1:9292');
    }

    // }}}
    // {{{ public function testSetMetadataBrokerListEmpty()

    /**
     * testSetMetadataBrokerListEmpty
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set broker list value is invalid, must is not empty string
     * @access public
     * @return void
     */
    public function testSetMetadataBrokerListEmpty()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataBrokerList('');
    }

    // }}}
    // {{{ public function testSetMetadataBrokerListEmpty1()

    /**
     * testSetMetadataBrokerListEmpty
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set broker list value is invalid, must is not empty string
     * @access public
     * @return void
     */
    public function testSetMetadataBrokerListEmpty1()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataBrokerList(',');
    }

    // }}}
    // {{{ public function testSetMetadataBrokerListEmpty2()

    /**
     * testSetMetadataBrokerListEmpty2
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set broker list value is invalid, must is not empty string
     * @access public
     * @return void
     */
    public function testSetMetadataBrokerListEmpty2()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataBrokerList('127.0.0.1,:');
    }

    // }}}
    // {{{ public function testSetGroupId()

    /**
     * testSetGroupId
     *
     * @access public
     * @return void
     */
    public function testSetGroupId()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setGroupId('test');
        $this->assertEquals($config->getGroupId(), 'test');
    }

    // }}}
    // {{{ public function testSetGroupIdEmpty()

    /**
     * testSetGroupIdEmpty
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set group id value is invalid, must set it not empty string
     * @access public
     * @return void
     */
    public function testSetGroupIdEmpty()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setGroupId('');
    }

    // }}}
    // {{{ public function testGetGroupIdEmpty()

    /**
     * testGetGroupIdEmpty
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Get group id value is invalid, must set it not empty string
     * @access public
     * @return void
     */
    public function testGetGroupIdEmpty()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->getGroupId();
    }

    // }}}
    // {{{ public function testSetSessionTimeout()

    /**
     * testSetSessionTimeout
     *
     * @access public
     * @return void
     */
    public function testSetSessionTimeout()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setSessionTimeout(2000);
        $this->assertEquals($config->getSessionTimeout(), 2000);
    }

    // }}}
    // {{{ public function testSetSessionTimeoutValid()

    /**
     * testSetSessionTimeoutValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set session timeout value is invalid, must set it 1 .. 3600000
     * @access public
     * @return void
     */
    public function testSetSessionTimeoutValid()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setSessionTimeout('-1');
    }

    // }}}
    // {{{ public function testSetRebalanceTimeout()

    /**
     * testSetRebalanceTimeout
     *
     * @access public
     * @return void
     */
    public function testSetRebalanceTimeout()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setRebalanceTimeout(2000);
        $this->assertEquals($config->getRebalanceTimeout(), 2000);
    }

    // }}}
    // {{{ public function testSetRebalanceTimeoutValid()

    /**
     * testSetRebalanceTimeoutValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set rebalance timeout value is invalid, must set it 1 .. 3600000
     * @access public
     * @return void
     */
    public function testSetRebalanceTimeoutValid()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setRebalanceTimeout('-1');
    }

    // }}}
    // {{{ public function testSetOffsetReset()

    /**
     * testSetOffsetReset
     *
     * @access public
     * @return void
     */
    public function testSetOffsetReset()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setOffsetReset('earliest');
        $this->assertEquals($config->getOffsetReset(), 'earliest');
    }

    // }}}
    // {{{ public function testSetOffsetResetValid()

    /**
     * testSetOffsetResetValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set offset reset value is invalid, must set it `latest` or `earliest`
     * @access public
     * @return void
     */
    public function testSetOffsetResetValid()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setOffsetReset('xxxx');
    }

    // }}}
    // {{{ public function testSetTopics()

    /**
     * testSetTopics
     *
     * @access public
     * @return void
     */
    public function testSetTopics()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setTopics(array('test'));
        $this->assertEquals($config->getTopics(), array('test'));
    }

    // }}}
    // {{{ public function testSetTopicsEmpty()

    /**
     * testSetTopicsEmpty
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set consumer topics value is invalid, must set it not empty array
     * @access public
     * @return void
     */
    public function testSetTopicsEmpty()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setTopics('');
    }

    // }}}
    // {{{ public function testGetTopicsEmpty()

    /**
     * testGetTopicsEmpty
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Get consumer topics value is invalid, must set it not empty
     * @access public
     * @return void
     */
    public function testGetTopicsEmpty()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->getTopics();
    }

    // }}}
    // }}}
}
