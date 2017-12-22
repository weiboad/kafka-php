<?php
namespace Kafka;

/**
 * @method string getClientId()
 * @method string getBrokerVersion()
 * @method string getMetadataBrokerList()
 * @method string getMessageMaxBytes()
 * @method string getMetadataRequestTimeoutMs()
 * @method string getMetadataRefreshIntervalMs()
 * @method string getMetadataMaxAgeMs()
 * @method string getSecurityProtocol()
 * @method bool getSslEnable()
 * @method void setSslEnable(bool $sslEnable)
 * @method string getSslLocalCert()
 * @method string getSslLocalPk()
 * @method bool getSslVerifyPeer()
 * @method void setSslVerifyPeer(bool $sslVerifyPeer)
 * @method string getSslPassphrase()
 * @method void setSslPassphrase(string $sslPassphrase)
 * @method string getSslCafile()
 * @method string getSslPeerName()
 * @method void setSslPeerName(string $sslPeerName)
 * @method string getSaslMechanism()
 * @method string getSaslUsername()
 * @method string getSaslPassword()
 * @method string getSaslKeytab()
 * @method string getSaslPrincipal()
 */
abstract class Config
{
    public const SECURITY_PROTOCOL_PLAINTEXT      = 'PLAINTEXT';
    public const SECURITY_PROTOCOL_SSL            = 'SSL';
    public const SECURITY_PROTOCOL_SASL_PLAINTEXT = 'SASL_PLAINTEXT';
    public const SECURITY_PROTOCOL_SASL_SSL       = 'SASL_SSL';

    public const SASL_MECHANISMS_PLAIN         = 'PLAIN';
    public const SASL_MECHANISMS_GSSAPI        = 'GSSAPI';
    public const SASL_MECHANISMS_SCRAM_SHA_256 = 'SCRAM_SHA_256';
    public const SASL_MECHANISMS_SCRAM_SHA_512 = 'SCRAM_SHA_512';

    private const ALLOW_SECURITY_PROTOCOLS = [
        self::SECURITY_PROTOCOL_PLAINTEXT,
        self::SECURITY_PROTOCOL_SSL,
        self::SECURITY_PROTOCOL_SASL_PLAINTEXT,
        self::SECURITY_PROTOCOL_SASL_SSL
    ];

    private const ALLOW_MECHANISMS = [
        self::SASL_MECHANISMS_PLAIN,
        self::SASL_MECHANISMS_GSSAPI,
        self::SASL_MECHANISMS_SCRAM_SHA_256,
        self::SASL_MECHANISMS_SCRAM_SHA_512
    ];

    protected static $options = [];

    private static $defaults = [
        'clientId'                  => 'kafka-php',
        'brokerVersion'             => '0.10.1.0',
        'metadataBrokerList'        => '',
        'messageMaxBytes'           => '1000000',
        'metadataRequestTimeoutMs'  => '60000',
        'metadataRefreshIntervalMs' => '300000',
        'metadataMaxAgeMs'          => -1,
        'securityProtocol'          => self::SECURITY_PROTOCOL_PLAINTEXT,
        'sslEnable'                 => false, // this config item will override, don't config it.
        'sslLocalCert'              => '',
        'sslLocalPk'                => '',
        'sslVerifyPeer'             => false,
        'sslPassphrase'             => '',
        'sslCafile'                 => '',
        'sslPeerName'               => '',
        'saslMechanism'             => self::SASL_MECHANISMS_PLAIN,
        'saslUsername'              => '',
        'saslPassword'              => '',
        'saslKeytab'                => '',
        'saslPrincipal'             => '',
    ];

    public function __call($name, $args)
    {
        if (strpos($name, 'get') === 0 || strpos($name, 'iet') === 0) {
            $option = strtolower(substr($name, 3, 1)) . substr($name, 4);
            if (isset(self::$options[$option])) {
                return self::$options[$option];
            }

            if (isset(self::$defaults[$option])) {
                return self::$defaults[$option];
            }
            if (isset(static::$defaults[$option])) {
                return static::$defaults[$option];
            }
            return false;
        }

        if (strpos($name, 'set') === 0) {
            if (count($args) != 1) {
                return false;
            }
            $option                   = strtolower(substr($name, 3, 1)) . substr($name, 4);
            static::$options[$option] = $args[0];
            // check todo
            return true;
        }
    }

    public function setClientId(string $val): void
    {
        $client = trim($val);

        if ($client === '') {
            throw new Exception\Config('Set clientId value is invalid, must is not empty string.');
        }

        static::$options['clientId'] = $client;
    }

    public function setBrokerVersion(string $version): void
    {
        $version = trim($version);

        if ($version === '' || version_compare($version, '0.8.0', '<')) {
            throw new Exception\Config('Set broker version value is invalid, must is not empty string and gt 0.8.0.');
        }

        static::$options['brokerVersion'] = $version;
    }

    public function setMetadataBrokerList(string $brokerList): void
    {
        $brokerList = trim($brokerList);

        $brokers = array_filter(
            explode(',', $brokerList),
            function (string $broker): bool {
                return preg_match('/^(.*:[\d]+)$/', $broker) === 1;
            }
        );

        if (empty($brokers)) {
            throw new Exception\Config(
                'Broker list must be a comma-separated list of brokers (format: "host:port"), with at least one broker'
            );
        }

        static::$options['metadataBrokerList'] = $brokerList;
    }

    public function clear(): void
    {
        static::$options = [];
    }

    public function setMessageMaxBytes($messageMaxBytes): void
    {
        if (! is_numeric($messageMaxBytes) || $messageMaxBytes < 1000 || $messageMaxBytes > 1000000000) {
            throw new Exception\Config('Set message max bytes value is invalid, must set it 1000 .. 1000000000');
        }
        static::$options['messageMaxBytes'] = $messageMaxBytes;
    }

    public function setMetadataRequestTimeoutMs($metadataRequestTimeoutMs): void
    {
        if (! is_numeric($metadataRequestTimeoutMs) || $metadataRequestTimeoutMs < 10
            || $metadataRequestTimeoutMs > 900000) {
            throw new Exception\Config('Set metadata request timeout value is invalid, must set it 10 .. 900000');
        }
        static::$options['metadataRequestTimeoutMs'] = $metadataRequestTimeoutMs;
    }

    public function setMetadataRefreshIntervalMs($metadataRefreshIntervalMs): void
    {
        if (! is_numeric($metadataRefreshIntervalMs) || $metadataRefreshIntervalMs < 10
            || $metadataRefreshIntervalMs > 3600000) {
            throw new Exception\Config('Set metadata refresh interval value is invalid, must set it 10 .. 3600000');
        }
        static::$options['metadataRefreshIntervalMs'] = $metadataRefreshIntervalMs;
    }

    public function setMetadataMaxAgeMs($metadataMaxAgeMs): void
    {
        if (! is_numeric($metadataMaxAgeMs) || $metadataMaxAgeMs < 1 || $metadataMaxAgeMs > 86400000) {
            throw new Exception\Config('Set metadata max age value is invalid, must set it 1 .. 86400000');
        }
        static::$options['metadataMaxAgeMs'] = $metadataMaxAgeMs;
    }

    public function setSslLocalCert(string $localCert): void
    {
        if (! file_exists($localCert) || ! is_file($localCert)) {
            throw new Exception\Config('Set ssl local cert file is invalid');
        }

        static::$options['sslLocalCert'] = $localCert;
    }

    public function setSslLocalPk(string $localPk): void
    {
        if (! file_exists($localPk) || ! is_file($localPk)) {
            throw new Exception\Config('Set ssl local private key file is invalid');
        }

        static::$options['sslLocalPk'] = $localPk;
    }

    public function setSslCafile(string $cafile): void
    {
        if (! file_exists($cafile) || ! is_file($cafile)) {
            throw new Exception\Config('Set ssl ca file is invalid');
        }

        static::$options['sslCafile'] = $cafile;
    }

    public function setSaslKeytab(string $keytab): void
    {
        if (! file_exists($keytab) || ! is_file($keytab)) {
            throw new Exception\Config('Set sasl gssapi keytab file is invalid');
        }

        static::$options['saslKeytab'] = $keytab;
    }

    public function setSecurityProtocol(string $protocol): void
    {
        if (! \in_array($protocol, self::ALLOW_SECURITY_PROTOCOLS, true)) {
            throw new Exception\Config('Invalid security protocol given.');
        }

        static::$options['securityProtocol'] = $protocol;
    }

    public function setSaslMechanism(string $mechanism): void
    {
        if (! \in_array($mechanism, self::ALLOW_MECHANISMS, true)) {
            throw new Exception\Config('Invalid security sasl mechanism given.');
        }

        static::$options['saslMechanism'] = $mechanism;
    }
}
