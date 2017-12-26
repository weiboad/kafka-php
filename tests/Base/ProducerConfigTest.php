<?php

namespace KafkaTest\Base;

use Kafka\Config;
use Kafka\ProducerConfig;
use Kafka\Protocol\Produce;
use PHPUnit\Framework\TestCase;

final class ProducerConfigTest extends TestCase
{
    /**
     * @var ProducerConfig
     */
    private $config;

    /**
     * @before
     */
    public function configureInstance(): void
    {
        $this->config = ProducerConfig::getInstance();
    }

    /**
     * @after
     */
    public function cleanUpInstance(): void
    {
        ProducerConfig::getInstance()->clear();
    }

    public function testSetRequestTimeout(): void
    {
        $this->config->setRequestTimeout(1011);

        self::assertSame($this->config->getRequestTimeout(), 1011);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set request timeout value is invalid, must set it 1 .. 900000
     */
    public function testSetRequestTimeoutValid(): void
    {
        $this->config->setRequestTimeout('-1');
    }

    public function testSetProduceInterval(): void
    {
        $this->config->setProduceInterval(1011);

        self::assertSame($this->config->getProduceInterval(), 1011);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set produce interval timeout value is invalid, must set it 1 .. 900000
     */
    public function testSetProduceIntervalValid(): void
    {
        $this->config->setProduceInterval('-1');
    }

    public function testSetTimeout(): void
    {
        $this->config->setTimeout(1011);

        self::assertSame($this->config->getTimeout(), 1011);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set timeout value is invalid, must set it 1 .. 900000
     */
    public function testSetTimeoutValid(): void
    {
        $this->config->setTimeout('-1');
    }

    public function testSetRequiredAck(): void
    {
        $this->config->setRequiredAck(1);
        self::assertSame($this->config->getRequiredAck(), 1);
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set required ack value is invalid, must set it -1 .. 1000
     */
    public function testSetRequiredAckValid(): void
    {
        $this->config->setRequiredAck('-2');
    }

    public function testSetIsAsyn(): void
    {
        $this->config->setIsAsyn(true);

        $this->assertTrue($this->config->getIsAsyn());
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set isAsyn value is invalid, must set it bool value
     */
    public function testSetIsAsynValid(): void
    {
        $this->config->setIsAsyn('-2');
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set ssl local cert file is invalid
     */
    public function testSetSslLocalCert(): void
    {
        $this->config->setSslLocalCert('invalid_path');
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set ssl local cert file is invalid
     */
    public function testSetSslLocalCertNotFile(): void
    {
        $this->config->setSslLocalCert('/tmp');
    }

    public function testSetSslLocalCertValid(): void
    {
        $path = '/etc/passwd';
        $this->config->setSslLocalCert($path);

        self::assertSame($path, $this->config->getSslLocalCert());
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set ssl local private key file is invalid
     */
    public function testSetSslLocalPk(): void
    {
        $this->config->setSslLocalPk('invalid_path');
    }

    public function testSetSslLocalPkValid(): void
    {
        $path = '/etc/passwd';
        $this->config->setSslLocalPk($path);

        self::assertSame($path, $this->config->getSslLocalPk());
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set ssl ca file is invalid
     */
    public function testSetSslCafile(): void
    {
        $this->config->setSslCafile('invalid_path');
    }

    public function testSetSslCafileValid(): void
    {
        $path = '/etc/passwd';
        $this->config->setSslCafile($path);

        self::assertSame($path, $this->config->getSslCafile());
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set sasl gssapi keytab file is invalid
     */
    public function testSetSaslKeytab(): void
    {
        $this->config->setSaslKeytab('invalid_path');
    }

    public function testSetSaslKeytabValid(): void
    {
        $path = '/etc/passwd';
        $this->config->setSaslKeytab($path);

        self::assertSame($path, $this->config->getSaslKeytab());
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Invalid security protocol given.
     */
    public function testSetSecurityProtocol(): void
    {
        $this->config->setSecurityProtocol('xxxx');
    }

    public function testSetSecurityProtocolValid(): void
    {
        $protocol = Config::SECURITY_PROTOCOL_PLAINTEXT;
        $this->config->setSecurityProtocol($protocol);

        self::assertSame($protocol, $this->config->getSecurityProtocol());
    }

    /**
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Invalid security sasl mechanism given.
     */
    public function testSetSaslMechanism(): void
    {
        $this->config->setSaslMechanism('xxxx');
    }

    public function testSetSaslMechanismValid(): void
    {
        $mechanism = Config::SASL_MECHANISMS_GSSAPI;
        $this->config->setSaslMechanism($mechanism);

        self::assertSame($mechanism, $this->config->getSaslMechanism());
    }

    /**
     * @test
     */
    public function defaultValueForCompressionIsNull(): void
    {
        self::assertSame(Produce::COMPRESSION_NONE, $this->config->getCompression());
    }

    /**
     * @test
     */
    public function compressionCanBeConfigured(): void
    {
        $this->config->setCompression(Produce::COMPRESSION_GZIP);

        self::assertSame(Produce::COMPRESSION_GZIP, $this->config->getCompression());
    }

    /**
     * @test
     */
    public function compressionCanOnlyBeConfiguredUsingAValidOption(): void
    {
        $this->config->setCompression(Produce::COMPRESSION_GZIP);

        self::assertSame(Produce::COMPRESSION_GZIP, $this->config->getCompression());
    }

    public function testClear(): void
    {
        $mechanism = Config::SASL_MECHANISMS_GSSAPI;
        $this->config->setSaslMechanism($mechanism);
        self::assertSame($mechanism, $this->config->getSaslMechanism());
        $this->config->clear();

        self::assertSame(Config::SASL_MECHANISMS_PLAIN, $this->config->getSaslMechanism());
    }
}
