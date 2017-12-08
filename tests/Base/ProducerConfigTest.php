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

namespace KafkaTest\Base;

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

class ProducerConfigTest extends \PHPUnit\Framework\TestCase
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
    // {{{ public function testSetSslLocalCert()

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
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setSslLocalCert('invalid_path');
    }

    // }}}
    // {{{ public function testSetSslLocalCertNotFile()

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
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setSslLocalCert('/tmp');
    }

    // }}}
    // {{{ public function testSetSslLocalCertValid()

    /**
     * testSetSslLocalCertValid
     *
     * @access public
     * @return void
     */
    public function testSetSslLocalCertValid()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $path   = '/etc/passwd';
        $config->setSslLocalCert($path);
        $this->assertEquals($path, $config->getSslLocalCert());
    }

    // }}}
    // {{{ public function testSetSslLocalPk()

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
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setSslLocalPk('invalid_path');
    }

    // }}}
    // {{{ public function testSetSslLocalPkValid()

    /**
     * testSetSslLocalPkValid
     *
     * @access public
     * @return void
     */
    public function testSetSslLocalPkValid()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $path   = '/etc/passwd';
        $config->setSslLocalPk($path);
        $this->assertEquals($path, $config->getSslLocalPk());
    }

    // }}}
    // {{{ public function testSetSslCafile()

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
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setSslCafile('invalid_path');
    }

    // }}}
    // {{{ public function testSetSslCafileValid()

    /**
     * testSetSslCafile
     *
     * @access public
     * @return void
     */
    public function testSetSslCafileValid()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $path   = '/etc/passwd';
        $config->setSslCafile($path);
        $this->assertEquals($path, $config->getSslCafile());
    }

    // }}}
    // {{{ public function testSetSaslKeytab()

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
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setSaslKeytab('invalid_path');
    }

    // }}}
    // {{{ public function testSetSaslKeytabValid()

    /**
     * testSetSaslKeytab
     *
     * @access public
     * @return void
     */
    public function testSetSaslKeytabValid()
    {
        $config = \Kafka\ProducerConfig::getInstance();
        $path   = '/etc/passwd';
        $config->setSaslKeytab($path);
        $this->assertEquals($path, $config->getSaslKeytab());
    }

    // }}}
    // {{{ public function testSetSecurityProtocol()

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
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setSecurityProtocol('xxxx');
    }

    // }}}
    // {{{ public function testSetSecurityProtocolValid()

    /**
     * testSetSecurityProtocol
     *
     * @access public
     * @return void
     */
    public function testSetSecurityProtocolValid()
    {
        $config   = \Kafka\ProducerConfig::getInstance();
        $protocol = \Kafka\Config::SECURITY_PROTOCOL_PLAINTEXT;
        $config->setSecurityProtocol($protocol);
        $this->assertEquals($protocol, $config->getSecurityProtocol());
    }

    // }}}
    // {{{ public function testSetSaslMechanism()

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
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setSaslMechanism('xxxx');
    }

    // }}}
    // {{{ public function testSetSaslMechanismValid()

    /**
     * testSetSaslMechanism
     *
     * @access public
     * @return void
     */
    public function testSetSaslMechanismValid()
    {
        $config    = \Kafka\ProducerConfig::getInstance();
        $mechanism = \Kafka\Config::SASL_MECHANISMS_GSSAPI;
        $config->setSaslMechanism($mechanism);
        $this->assertEquals($mechanism, $config->getSaslMechanism());
    }

    // }}}
    // {{{ public function testClear()

    /**
     * testClear
     *
     * @access public
     * @return void
     */
    public function testClear()
    {
        $config    = \Kafka\ProducerConfig::getInstance();
        $mechanism = \Kafka\Config::SASL_MECHANISMS_GSSAPI;
        $config->setSaslMechanism($mechanism);
        $this->assertEquals($mechanism, $config->getSaslMechanism());
        $config->clear();
        $this->assertEquals(\Kafka\Config::SASL_MECHANISMS_PLAIN, $config->getSaslMechanism());
    }

    // }}}
    // }}}
}
