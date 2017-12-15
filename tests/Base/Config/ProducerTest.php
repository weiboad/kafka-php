<?php
namespace KafkaTest\Base\Config;

use \Kafka\Config\Producer;

class ProducerTest extends \PHPUnit\Framework\TestCase
{
    private $config = null;

    protected function setUp()
    {
        $this->config = new Producer();
    }

    /**
     * testSetRequestTimeout
     *
     * @access public
     * @return void
     */
    public function testSetRequestTimeout()
    {
        $this->config->setRequestTimeout(1011);
        $this->assertEquals($this->config->getRequestTimeout(), 1011);
    }

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
        $this->config->setRequestTimeout(-1);
    }

    /**
     * testSetProduceInterval
     *
     * @access public
     * @return void
     */
    public function testSetProduceInterval()
    {
        $this->config->setProduceInterval(1011);
        $this->assertEquals($this->config->getProduceInterval(), 1011);
    }

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
        $this->config->setProduceInterval(-1);
    }

    /**
     * testSetTimeout
     *
     * @access public
     * @return void
     */
    public function testSetTimeout()
    {
        $this->config->setTimeout(1011);
        $this->assertEquals($this->config->getTimeout(), 1011);
    }

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
        $this->config->setTimeout('-1');
    }

    /**
     * testSetRequiredAck
     *
     * @access public
     * @return void
     */
    public function testSetRequiredAck()
    {
        $this->config->setRequiredAck(1);
        $this->assertEquals($this->config->getRequiredAck(), 1);
    }

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
        $this->config->setRequiredAck(-2);
    }
}
