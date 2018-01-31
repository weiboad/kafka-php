<?php
namespace KafkaTest\Base;

class ConsumerConfigTest extends \PHPUnit\Framework\TestCase
{

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
        $this->assertFalse($config->getValidKey());
        $config->setValidKey('222');
        $this->assertEquals($config->getValidKey(), '222');
    }

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

    /**
     * testSetBrokerVersionEmpty
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set broker version value is invalid, must is not empty and gt 0.8.0.
     * @access public
     * @return void
     */
    public function testSetBrokerVersionEmpty()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setBrokerVersion('');
    }

    /**
     * testSetBrokerVersionValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set broker version value is invalid, must is not empty and gt 0.8.0.
     * @access public
     * @return void
     */
    public function testSetBrokerVersionValid()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setBrokerVersion('0.1');
    }

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
        $config->setMetadataBrokerList('127.0.0.1: , : ');
    }

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

    /**
     * testSetTopics
     *
     * @access public
     * @return void
     */
    public function testSetTopics()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setTopics(['test']);
        $this->assertEquals($config->getTopics(), ['test']);
    }

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

    /**
     * testSetMessageMaxBytes
     *
     * @access public
     * @return void
     */
    public function testSetMessageMaxBytes()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMessageMaxBytes(1011);
        $this->assertEquals($config->getMessageMaxBytes(), 1011);
    }

    /**
     * testSetMessageMaxBytesValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set message max bytes value is invalid, must set it 1000 .. 1000000000
     * @access public
     * @return void
     */
    public function testSetMessageMaxBytesValid()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMessageMaxBytes('999');
    }

    /**
     * testSetMetadataRequestTimeoutMs
     *
     * @access public
     * @return void
     */
    public function testSetMetadataRequestTimeoutMs()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataRequestTimeoutMs(1011);
        $this->assertEquals($config->getMetadataRequestTimeoutMs(), 1011);
    }

    /**
     * testSetMetadataRequestTimeoutMsValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set metadata request timeout value is invalid, must set it 10 .. 900000
     * @access public
     * @return void
     */
    public function testSetMetadataRequestTimeoutMsValid()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataRequestTimeoutMs('9');
    }

    /**
     * testSetMetadataRefreshIntervalMs
     *
     * @access public
     * @return void
     */
    public function testSetMetadataRefreshIntervalMs()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataRefreshIntervalMs(1011);
        $this->assertEquals($config->getMetadataRefreshIntervalMs(), 1011);
    }

    /**
     * testSetMetadataRefreshIntervalMsValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set metadata refresh interval value is invalid, must set it 10 .. 3600000
     * @access public
     * @return void
     */
    public function testSetMetadataRefreshIntervalMsValid()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataRefreshIntervalMs('9');
    }

    /**
     * testSetMetadataMaxAgeMs
     *
     * @access public
     * @return void
     */
    public function testSetMetadataMaxAgeMs()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataMaxAgeMs(1011);
        $this->assertEquals($config->getMetadataMaxAgeMs(), 1011);
    }

    /**
     * testSetMetadataMaxAgeMsValid
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set metadata max age value is invalid, must set it 1 .. 86400000
     * @access public
     * @return void
     */
    public function testSetMetadataMaxAgeMsValid()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataMaxAgeMs('86400001');
    }
}
