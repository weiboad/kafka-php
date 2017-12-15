<?php
namespace KafkaTest\Base\Config;

use \Kafka\Config\Ssl;

class SslTest extends \PHPUnit\Framework\TestCase
{
    private $config = null;

    protected function setUp()
    {
        $this->config = new Ssl();
    }

    /**
     * testSetSslLocalCert
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set ssl local cert file is invalid
     * @access public
     * @return void
     */
    public function testSetSslLocalCert()
    {
        $this->config->setLocalCert('invalid_path');
    }

    /**
     * testSetSslLocalCert
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set ssl local cert file is invalid
     * @access public
     * @return void
     */
    public function testSetSslLocalCertNotFile()
    {
        $this->config->setLocalCert('/tmp');
    }

    /**
     * testSetSslLocalCertValid
     *
     * @access public
     * @return void
     */
    public function testSetSslLocalCertValid()
    {
        $path = '/etc/passwd';
        $this->config->setLocalCert($path);
        $this->assertEquals($path, $this->config->getLocalCert());
    }

    /**
     * testSetSslLocalPk
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set ssl local private key file is invalid
     * @access public
     * @return void
     */
    public function testSetSslLocalPk()
    {
        $this->config->setLocalPk('invalid_path');
    }

    /**
     * testSetSslLocalPkValid
     *
     * @access public
     * @return void
     */
    public function testSetSslLocalPkValid()
    {
        $path = '/etc/passwd';
        $this->config->setLocalPk($path);
        $this->assertEquals($path, $this->config->getLocalPk());
    }

    /**
     * testSetSslCafile
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set ssl ca file is invalid
     * @access public
     * @return void
     */
    public function testSetSslCafile()
    {
        $this->config->setCafile('invalid_path');
    }

    /**
     * testSetSslCafile
     *
     * @access public
     * @return void
     */
    public function testSetSslCafileValid()
    {
        $path = '/etc/passwd';
        $this->config->setCafile($path);
        $this->assertEquals($path, $this->config->getCafile());
    }
}
