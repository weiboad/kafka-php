<?php
namespace KafkaTest\Base\Config;

use \Kafka\Config\Sasl;

class SaslTest extends \PHPUnit\Framework\TestCase
{
    private $config = null;

    protected function setUp()
    {
        $this->config = new Sasl();
    }

    /**
     * testSetSaslKeytab
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set sasl gssapi keytab file is invalid
     * @access public
     * @return void
     */
    public function testSetSaslKeytab()
    {
        $this->config->setKeytab('invalid_path');
    }

    /**
     * testSetSaslKeytab
     *
     * @access public
     * @return void
     */
    public function testSetSaslKeytabValid()
    {
        $path = '/etc/passwd';
        $this->config->setKeytab($path);
        $this->assertEquals($path, $this->config->getKeytab());
    }

    public function testSetAndGet()
    {
        $this->config->setPrincipal('xxxx');
        $this->assertEquals('xxxx', $this->config->getPrincipal());

        $this->config->setUsername('xxxx');
        $this->assertEquals('xxxx', $this->config->getUsername());
        $this->config->setPassword('xxxx');
        $this->assertEquals('xxxx', $this->config->getPassword());
    }
}
