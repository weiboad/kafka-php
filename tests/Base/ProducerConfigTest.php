<?php
namespace KafkaTest\Base;

class ProducerConfigTest extends \PHPUnit\Framework\TestCase
{

    /**
     * setDown
     *
     * @access public
     * @return void
     */
    public function tearDown()
    {
        \Kafka\lib\ProducerConfig::getInstance()->clear();
    }

    /**
     * testSetRequestTimeout
     *
     * @access public
     * @return void
     */
    public function testSetRequestTimeout()
    {
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setRequestTimeout(1011);
        $this->assertEquals($config->getRequestTimeout(), 1011);
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
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setRequestTimeout('-1');
    }

    /**
     * testSetProduceInterval
     *
     * @access public
     * @return void
     */
    public function testSetProduceInterval()
    {
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setProduceInterval(1011);
        $this->assertEquals($config->getProduceInterval(), 1011);
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
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setProduceInterval('-1');
    }

    /**
     * testSetTimeout
     *
     * @access public
     * @return void
     */
    public function testSetTimeout()
    {
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setTimeout(1011);
        $this->assertEquals($config->getTimeout(), 1011);
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
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setTimeout('-1');
    }

    /**
     * testSetRequiredAck
     *
     * @access public
     * @return void
     */
    public function testSetRequiredAck()
    {
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setRequiredAck(1);
        $this->assertEquals($config->getRequiredAck(), 1);
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
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setRequiredAck('-2');
    }

    /**
     * testSetIsAsyn
     *
     * @access public
     * @return void
     */
    public function testSetIsAsyn()
    {
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setIsAsyn(true);
        $this->assertTrue($config->getIsAsyn());
    }

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
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setIsAsyn('-2');
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
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setSslLocalCert('invalid_path');
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
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setSslLocalCert('/tmp');
    }

    /**
     * testSetSslLocalCertValid
     *
     * @access public
     * @return void
     */
    public function testSetSslLocalCertValid()
    {
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $path   = '/etc/passwd';
        $config->setSslLocalCert($path);
        $this->assertEquals($path, $config->getSslLocalCert());
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
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setSslLocalPk('invalid_path');
    }

    /**
     * testSetSslLocalPkValid
     *
     * @access public
     * @return void
     */
    public function testSetSslLocalPkValid()
    {
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $path   = '/etc/passwd';
        $config->setSslLocalPk($path);
        $this->assertEquals($path, $config->getSslLocalPk());
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
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setSslCafile('invalid_path');
    }

    /**
     * testSetSslCafile
     *
     * @access public
     * @return void
     */
    public function testSetSslCafileValid()
    {
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $path   = '/etc/passwd';
        $config->setSslCafile($path);
        $this->assertEquals($path, $config->getSslCafile());
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
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setSaslKeytab('invalid_path');
    }

    /**
     * testSetSaslKeytab
     *
     * @access public
     * @return void
     */
    public function testSetSaslKeytabValid()
    {
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $path   = '/etc/passwd';
        $config->setSaslKeytab($path);
        $this->assertEquals($path, $config->getSaslKeytab());
    }

    /**
     * testSetSecurityProtocol
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Invalid security protocol given.
     * @access public
     * @return void
     */
    public function testSetSecurityProtocol()
    {
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setSecurityProtocol('xxxx');
    }

    /**
     * testSetSecurityProtocol
     *
     * @access public
     * @return void
     */
    public function testSetSecurityProtocolValid()
    {
        $config   = \Kafka\lib\ProducerConfig::getInstance();
        $protocol = \Kafka\lib\Config::SECURITY_PROTOCOL_PLAINTEXT;
        $config->setSecurityProtocol($protocol);
        $this->assertEquals($protocol, $config->getSecurityProtocol());
    }

    /**
     * testSetSaslMechanism
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Invalid security sasl mechanism given.
     * @access public
     * @return void
     */
    public function testSetSaslMechanism()
    {
        $config = \Kafka\lib\ProducerConfig::getInstance();
        $config->setSaslMechanism('xxxx');
    }

    /**
     * testSetSaslMechanism
     *
     * @access public
     * @return void
     */
    public function testSetSaslMechanismValid()
    {
        $config    = \Kafka\lib\ProducerConfig::getInstance();
        $mechanism = \Kafka\lib\Config::SASL_MECHANISMS_GSSAPI;
        $config->setSaslMechanism($mechanism);
        $this->assertEquals($mechanism, $config->getSaslMechanism());
    }

    /**
     * testClear
     *
     * @access public
     * @return void
     */
    public function testClear()
    {
        $config    = \Kafka\lib\ProducerConfig::getInstance();
        $mechanism = \Kafka\lib\Config::SASL_MECHANISMS_GSSAPI;
        $config->setSaslMechanism($mechanism);
        $this->assertEquals($mechanism, $config->getSaslMechanism());
        $config->clear();
        $this->assertEquals(\Kafka\lib\Config::SASL_MECHANISMS_PLAIN, $config->getSaslMechanism());
    }
}
