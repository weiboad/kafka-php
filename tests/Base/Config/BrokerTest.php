<?php
namespace KafkaTest\Base\Config;

use \Kafka\Config\Broker;

class BrokerTest extends \PHPUnit\Framework\TestCase
{
    private $config = null;

    protected function setUp()
    {
        $this->config = new Broker();
    }

    /**
     * testDefaultConfig
     *
     * @access public
     * @return void
     */
    public function testDefaultConfig()
    {
        $this->assertEquals($this->config->getClientId(), 'kafka-php');
    }

    /**
     * testSetClientId
     *
     * @access public
     * @return void
     */
    public function testSetClientId()
    {
        $this->config->setClientId('kafka-php1');
        $this->assertEquals($this->config->getClientId(), 'kafka-php1');
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
        $this->config->setClientId('');
    }

    /**
     * testSetBrokerVersion
     *
     * @access public
     * @return void
     */
    public function testSetBrokerVersion()
    {
        $this->config->setVersion('0.9.0.1');
        $this->assertEquals($this->config->getVersion(), '0.9.0.1');
    }

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
        $this->config->setVersion('');
    }

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
        $this->config->setVersion('0.1');
    }

    /**
     * testSetMetadataBrokerList
     *
     * @access public
     * @return void
     */
    public function testSetMetadataBrokerList()
    {
        $this->config->setMetadataBrokerList('127.0.0.1:9192,127.0.0.1:9292');
        $this->assertEquals($this->config->getMetadataBrokerList(), '127.0.0.1:9192,127.0.0.1:9292');
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
        $this->config->setMetadataBrokerList('');
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
        $this->config->setMetadataBrokerList(',');
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
        $this->config->setMetadataBrokerList('127.0.0.1: , : ');
    }

    /**
     * testSetMessageMaxBytes
     *
     * @access public
     * @return void
     */
    public function testSetMessageMaxBytes()
    {
        $this->config->setMessageMaxBytes(1011);
        $this->assertEquals($this->config->getMessageMaxBytes(), 1011);
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
        $this->config->setMessageMaxBytes(999);
    }

    /**
     * testSetMetadataRequestTimeoutMs
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set metadata request timeout value is invalid, must set it 10 .. 900000
     * @access public
     * @return void
     */
    public function testSetMetadataRequestTimeoutMs()
    {
        $this->config->setMetadataRequestTimeoutMs(9);
    }

    /**
     * testSetMetadataRefreshIntervalMs
     *
     * @access public
     * @return void
     */
    public function testSetMetadataRefreshIntervalMs()
    {
        $this->config->setMetadataRefreshIntervalMs(1011);
        $this->assertEquals($this->config->getMetadataRefreshIntervalMs(), 1011);
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
        $this->config->setMetadataRefreshIntervalMs('9');
    }

    /**
     * testSetMetadataMaxAgeMs
     *
     * @access public
     * @return void
     */
    public function testSetMetadataMaxAgeMs()
    {
        $this->config->setMetadataMaxAgeMs(1011);
        $this->assertEquals($this->config->getMetadataMaxAgeMs(), 1011);
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
        $this->config->setMetadataMaxAgeMs('86400001');
    }
}
