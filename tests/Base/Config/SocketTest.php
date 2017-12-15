<?php
namespace KafkaTest\Base\Config;

use \Kafka\Config\Socket;

class SocketTest extends \PHPUnit\Framework\TestCase
{
    private $config = null;

    protected function setUp()
    {
        $this->config = new Socket();
    }

    public function testDefault()
    {
        $this->assertEquals(0, $this->config->getSendTimeoutSec());
        $this->assertEquals(100000, $this->config->getSendTimeoutUsec());
        $this->assertEquals(0, $this->config->getRecvTimeoutSec());
        $this->assertEquals(750000, $this->config->getRecvTimeoutUsec());
        $this->assertEquals(3, $this->config->getMaxWriteAttempts());
    }

    public function testAllSetGet()
    {
        $this->config->setRecvTimeoutSec(100);
        $this->assertEquals(100, $this->config->getRecvTimeoutSec());
        $this->config->setRecvTimeoutUsec(100);
        $this->assertEquals(100, $this->config->getRecvTimeoutUsec());
        $this->config->setMaxWriteAttempts(4);
        $this->assertEquals(4, $this->config->getMaxWriteAttempts());
        $this->config->setSendTimeoutSec(100);
        $this->assertEquals(100, $this->config->getSendTimeoutSec());
        $this->config->setSendTimeoutUsec(100);
        $this->assertEquals(100, $this->config->getSendTimeoutUsec());
    }
}
