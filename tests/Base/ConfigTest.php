<?php
declare(strict_types=1);

namespace KafkaTest\Base;

use Kafka\Config;
use PHPStan\Testing\TestCase;
use PHPUnit\Framework\MockObject\MockObject;

final class ConfigTest extends TestCase
{
    /**
     * @var Config|MockObject
     */
    private $config;

    /**
     * @before
     */
    public function createConfig(): void
    {
        $this->config = $this->getMockForAbstractClass(Config::class);
    }

    /**
     * @after
     */
    public function cleanUpInstance(): void
    {
        $this->config->clear();
    }

    /**
     * @test
     */
    public function defaultValuesShouldBeRetrievedWhenNothingWasConfigured(): void
    {
        self::assertSame('kafka-php', $this->config->getClientId());
        self::assertSame('0.10.1.0', $this->config->getBrokerVersion());
        self::assertSame('', $this->config->getMetadataBrokerList());
        self::assertSame(1000000, $this->config->getMessageMaxBytes());
        self::assertSame(60000, $this->config->getMetadataRequestTimeoutMs());
        self::assertSame(300000, $this->config->getMetadataRefreshIntervalMs());
        self::assertSame(-1, $this->config->getMetadataMaxAgeMs());
    }

    public function clearShouldResetConfigurationToItsDefaults(): void
    {
        $this->config->setClientId('my-client');
        $this->config->clear();

        self::assertSame('kafka-php', $this->config->getClientId());
    }

    /**
     * @test
     *
     * TODO: kill this with fire
     */
    public function randomDataCanBeConfiguredUsingMagicMethods(): void
    {
        self::assertFalse($this->config->setValidKey('xxx', '222'));
        self::assertFalse($this->config->getValidKey());

        $this->config->setValidKey('222');
        self::assertSame($this->config->getValidKey(), '222');
    }

    /**
     * @test
     *
     * TODO: kill this with fire
     */
    public function magicMethodShouldReturnFalseWhenCallingAMethodThatIsNeitherGetterOrSetter(): void
    {
        self::assertFalse($this->config->pureMagic());
    }

    /**
     * @test
     */
    public function setClientIdShouldConfigureTheAttributeProperly(): void
    {
        $this->config->setClientId('kafka-php1');

        self::assertSame('kafka-php1', $this->config->getClientId());
    }

    /**
     * @test
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set clientId value is invalid, must is not empty string.
     */
    public function setClientIdShouldRaiseAnExceptionWhenIdIsEmpty(): void
    {
        $this->config->setClientId('');
    }

    /**
     * @test
     */
    public function setBrokerVersionShouldConfigureTheAttributeProperly(): void
    {
        $this->config->setBrokerVersion('0.9.0.1');

        self::assertSame('0.9.0.1', $this->config->getBrokerVersion());
    }

    /**
     * @test
     *
     * @dataProvider invalidBrokerVersion
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set broker version value is invalid, must is not empty string and gt 0.8.0.
     */
    public function setBrokerVersionShouldRaiseExceptionWhenInvalidDataIsGiven(string $brokerVersion): void
    {
        $this->config->setBrokerVersion($brokerVersion);
    }

    /**
     * @return string[][]
     */
    public function invalidBrokerVersion(): array
    {
        return [
            [''],
            ['0.1'],
            ['0.2'],
            ['0.3'],
            ['0.4'],
            ['0.5'],
            ['0.6'],
            ['0.7'],
        ];
    }

    /**
     * @test
     */
    public function setMetadataBrokerListShouldTrimSpacesFromGivenData(): void
    {
        $this->config->setMetadataBrokerList(' 127.0.0.1:9192,127.0.0.1:9292 '); // with whitespace to ensure that the list is trimmed

        self::assertSame('127.0.0.1:9192,127.0.0.1:9292', $this->config->getMetadataBrokerList());
    }

    /**
     * @test
     *
     * @dataProvider invalidBrokerList
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Broker list must be a comma-separated list of brokers (format: "host:port"), with at least one broker
     */
    public function setMetadataBrokerListShouldRaiseAnExceptionWhenInvalidDataIsGiven(string $brokerList): void
    {
        $this->config->setMetadataBrokerList($brokerList);
    }

    /**
     * @return string[][]
     */
    public function invalidBrokerList(): array
    {
        return [
            [''],
            [','],
            ['127.0.0.1: , : '],
        ];
    }

    /**
     * @test
     */
    public function setMessageMaxBytesShouldConfigureTheAttributeProperly(): void
    {
        $this->config->setMessageMaxBytes(1011);

        self::assertSame(1011, $this->config->getMessageMaxBytes());
    }

    /**
     * @test
     *
     * @dataProvider invalidMessageMaxBytes
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set message max bytes value is invalid, must set it 1000 .. 1000000000
     */
    public function setMessageMaxBytesShouldRaiseExceptionWhenInvalidDataIsGiven(int $messageMaxBytes): void
    {
        $this->config->setMessageMaxBytes($messageMaxBytes);
    }

    /**
     * @return int[][]
     */
    public function invalidMessageMaxBytes(): array
    {
        return [
            [999],
            [1000000001],
        ];
    }

    /**
     * @test
     */
    public function setMetadataRequestTimeoutMsShouldConfigureTheAttributeProperly(): void
    {
        $this->config->setMetadataRequestTimeoutMs(1011);

        self::assertSame(1011, $this->config->getMetadataRequestTimeoutMs());
    }

    /**
     * @test
     *
     * @dataProvider invalidMetadataRequestTimeoutMs
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set metadata request timeout value is invalid, must set it 10 .. 900000
     */
    public function testSetMetadataRequestTimeoutMsShouldRaiseExceptionWhenInvalidDataIsGiven(int $metadataRequestTimeoutMs): void
    {
        $this->config->setMetadataRequestTimeoutMs($metadataRequestTimeoutMs);
    }

    /**
     * @return int[][]
     */
    public function invalidMetadataRequestTimeoutMs(): array
    {
        return [
            [9],
            [900001],
        ];
    }

    /**
     * @test
     */
    public function setMetadataRefreshIntervalMsShouldConfigureTheAttributeProperly(): void
    {
        $this->config->setMetadataRefreshIntervalMs(1011);

        self::assertSame(1011, $this->config->getMetadataRefreshIntervalMs());
    }

    /**
     * @test
     *
     * @dataProvider invalidMetadataRefreshIntervalMs
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set metadata refresh interval value is invalid, must set it 10 .. 3600000
     */
    public function setMetadataRefreshIntervalMsShouldRaiseExceptionWhenInvalidDataIsGiven(int $metadataRefreshIntervalMs): void
    {
        $this->config->setMetadataRefreshIntervalMs($metadataRefreshIntervalMs);
    }

    /**
     * @return int[][]
     */
    public function invalidMetadataRefreshIntervalMs(): array
    {
        return [
            [9],
            [3600001],
        ];
    }

    /**
     * @test
     */
    public function setMetadataMaxAgeMsShouldConfigureTheAttributeProperly(): void
    {
        $this->config->setMetadataMaxAgeMs(1011);

        self::assertSame(1011, $this->config->getMetadataMaxAgeMs());
    }

    /**
     * @test
     *
     * @dataProvider invalidMetadataMaxAgeMs
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set metadata max age value is invalid, must set it 1 .. 86400000
     */
    public function setMetadataMaxAgeMsShouldRaiseExceptionWhenInvalidDataIsGiven(int $metadataMaxAgeMs): void
    {
        $this->config->setMetadataMaxAgeMs($metadataMaxAgeMs);
    }

    /**
     * @return int[][]
     */
    public function invalidMetadataMaxAgeMs(): array
    {
        return [
            [0],
            [86400001],
        ];
    }

    /**
     * @test
     */
    public function setSslLocalCertShouldConfigureTheAttributeProperly(): void
    {
        $this->config->setSslLocalCert('/etc/passwd');

        self::assertSame('/etc/passwd', $this->config->getSslLocalCert());
    }

    /**
     * @test
     *
     * @dataProvider invalidFiles
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set ssl local cert file is invalid
     */
    public function setSslLocalCertShouldRaiseExceptionWhenInvalidDataIsGiven(string $file): void
    {
        $this->config->setSslLocalCert($file);
    }

    /**
     * @test
     */
    public function setSslLocalPkShouldConfigureTheAttributeProperly(): void
    {
        $this->config->setSslLocalPk('/etc/passwd');

        self::assertSame('/etc/passwd', $this->config->getSslLocalPk());
    }

    /**
     * @test
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set ssl local private key file is invalid
     */
    public function setSslLocalPkShouldRaiseExceptionWhenInvalidDataIsGiven(): void
    {
        $this->config->setSslLocalPk('invalid_path');
    }

    /**
     * @test
     */
    public function setSslCafileShouldConfigureTheAttributeProperly(): void
    {
        $this->config->setSslCafile('/etc/passwd');

        self::assertSame('/etc/passwd', $this->config->getSslCafile());
    }

    /**
     * @test
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set ssl ca file is invalid
     */
    public function setSslCafileShouldRaiseExceptionWhenInvalidDataIsGiven(): void
    {
        $this->config->setSslCafile('invalid_path');
    }

    /**
     * @test
     */
    public function setSaslKeytabShouldConfigureTheAttributeProperly(): void
    {
        $this->config->setSaslKeytab('/etc/passwd');

        self::assertSame('/etc/passwd', $this->config->getSaslKeytab());
    }

    /**
     * @test
     *
     * @dataProvider invalidFiles
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Set sasl gssapi keytab file is invalid
     */
    public function setSaslKeytabShouldRaiseExceptionWhenInvalidDataIsGiven(string $file): void
    {
        $this->config->setSaslKeytab($file);
    }

    /**
     * @return string[][]
     */
    public function invalidFiles(): array
    {
        return [
            ['invalid_path'],
            ['/tmp'],
        ];
    }

    /**
     * @test
     */
    public function setSecurityProtocolShouldConfigureTheAttributeProperly(): void
    {
        $this->config->setSecurityProtocol(Config::SECURITY_PROTOCOL_PLAINTEXT);

        self::assertSame(Config::SECURITY_PROTOCOL_PLAINTEXT, $this->config->getSecurityProtocol());
    }

    /**
     * @test
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Invalid security protocol given.
     */
    public function setSecurityProtocolShouldRaiseExceptionWhenInvalidDataIsGiven(): void
    {
        $this->config->setSecurityProtocol('xxxx');
    }

    /**
     * @test
     */
    public function setSaslMechanismShouldConfigureTheAttributeProperly(): void
    {
        $mechanism = Config::SASL_MECHANISMS_GSSAPI;
        $this->config->setSaslMechanism($mechanism);

        self::assertSame($mechanism, $this->config->getSaslMechanism());
    }

    /**
     * @test
     *
     * @expectedException \Kafka\Exception\Config
     * @expectedExceptionMessage Invalid security sasl mechanism given.
     */
    public function setSaslMechanismShouldRaiseExceptionWhenInvalidDataIsGiven(): void
    {
        $this->config->setSaslMechanism('xxxx');
    }
}
