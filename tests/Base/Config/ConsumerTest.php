<?php
namespace KafkaTest\Base\Config;

use \Kafka\Config\Consumer;

class ConsumerTest extends \PHPUnit\Framework\TestCase
{
    private $config = null;

    protected function setUp()
    {
        $this->config = new Consumer();
    }

    /**
     * testDefaultConfig
     *
     * @access public
     * @return void
     */
    public function testDefaultConfig()
    {
        $this->assertEquals($this->config->getSessionTimeout(), 30000);
    }

    /**
     * testSetGroupId
     *
     * @access public
     * @return void
     */
    public function testSetGroupId()
    {
        $this->config->setGroupId('test');
        $this->assertEquals($this->config->getGroupId(), 'test');
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
        $this->config->setGroupId('');
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
        $this->config->getGroupId();
    }

    /**
     * testSetSessionTimeout
     *
     * @access public
     * @return void
     */
    public function testSetSessionTimeout()
    {
        $this->config->setSessionTimeout(2000);
        $this->assertEquals($this->config->getSessionTimeout(), 2000);
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
        $this->config->setSessionTimeout(-1);
    }

    /**
     * testSetRebalanceTimeout
     *
     * @access public
     * @return void
     */
    public function testSetRebalanceTimeout()
    {
        $this->config->setRebalanceTimeout(2000);
        $this->assertEquals($this->config->getRebalanceTimeout(), 2000);
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
        $this->config->setRebalanceTimeout(-1);
    }

    /**
     * testSetOffsetReset
     *
     * @access public
     * @return void
     */
    public function testSetOffsetReset()
    {
        $this->config->setOffsetReset('earliest');
        $this->assertEquals($this->config->getOffsetReset(), 'earliest');
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
        $this->config->setOffsetReset('xxxx');
    }

    /**
     * testSetTopics
     *
     * @access public
     * @return void
     */
    public function testSetTopics()
    {
        $this->config->setTopics(['test']);
        $this->assertEquals($this->config->getTopics(), ['test']);
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
        $this->config->setTopics([]);
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
        $this->config->getTopics();
    }

    public function testGetSet()
    {
        $this->config->setMaxBytes(1024);
        $this->assertEquals(1024, $this->config->getMaxBytes());
        $this->config->setMaxWaitTime(1024);
        $this->assertEquals(1024, $this->config->getMaxWaitTime());
        $this->config->setConsumeMode(\Kafka\Config\Consumer::CONSUME_BEFORE_COMMIT_OFFSET);
        $this->assertEquals(\Kafka\Config\Consumer::CONSUME_BEFORE_COMMIT_OFFSET, $this->config->getConsumeMode());
    }
}
