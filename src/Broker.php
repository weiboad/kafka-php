<?php
declare(strict_types=1);

namespace Kafka;

use Kafka\Sasl\Gssapi;
use Kafka\Sasl\Plain;
use Kafka\Sasl\Scram;
use function array_keys;
use function array_walk_recursive;
use function explode;
use function in_array;
use function serialize;
use function shuffle;
use function sprintf;
use function strpos;

class Broker
{
    use SingletonTrait;

    public const SOCKET_MODE_ASYNC = 0;
    public const SOCKET_MODE_SYNC  = 1;

    /**
     * @var int
     */
    private $groupBrokerId;

    /**
     * @var mixed[][]
     */
    private $topics = [];

    /**
     * @var string[]
     */
    private $brokers = [];

    /**
     * @var CommonSocket[]
     */
    private $metaSockets = [];

    /**
     * @var CommonSocket[]
     */
    private $dataSockets = [];

    /** @var SocketFactory */
    private $socketFactory;

    /**
     * @var callable|null
     */
    private $process;

    /**
     * @var Config|null
     */
    private $config;

    public function setSocketFactory(SocketFactory $socketFactory): void
    {
        $this->socketFactory = $socketFactory;
    }

    public function setProcess(callable $process): void
    {
        $this->process = $process;
    }

    public function setConfig(Config $config): void
    {
        $this->config = $config;
    }

    public function setGroupBrokerId(int $brokerId): void
    {
        $this->groupBrokerId = $brokerId;
    }

    public function getGroupBrokerId(): int
    {
        return $this->groupBrokerId;
    }

    /**
     * @param mixed[][] $topics
     * @param mixed[]   $brokersResult
     */
    public function setData(array $topics, array $brokersResult): bool
    {
        $brokers = [];

        foreach ($brokersResult as $value) {
            $brokers[$value['nodeId']] = $value['host'] . ':' . $value['port'];
        }

        $changed = false;

        if (serialize($this->brokers) !== serialize($brokers)) {
            $this->brokers = $brokers;

            $changed = true;
        }

        $newTopics = [];
        foreach ($topics as $topic) {
            if ((int) $topic['errorCode'] !== Protocol::NO_ERROR) {
                $this->error('Parse metadata for topic is error, error:' . Protocol::getError($topic['errorCode']));
                continue;
            }

            $item = [];

            foreach ($topic['partitions'] as $part) {
                $item[$part['partitionId']] = $part['leader'];
            }

            $newTopics[$topic['topicName']] = $item;
        }

        if (serialize($this->topics) !== serialize($newTopics)) {
            $this->topics = $newTopics;

            $changed = true;
        }

        return $changed;
    }

    /**
     * @return mixed[][]
     */
    public function getTopics(): array
    {
        return $this->topics;
    }

    /**
     * @return string[]
     */
    public function getBrokers(): array
    {
        return $this->brokers;
    }

    public function getMetaConnect(string $key, int $mode = self::SOCKET_MODE_ASYNC): ?CommonSocket
    {
        return $this->getConnect($key, 'metaSockets', $mode);
    }

    public function getRandConnect(int $mode = self::SOCKET_MODE_ASYNC): ?CommonSocket
    {
        $nodeIds = array_keys($this->brokers);
        shuffle($nodeIds);

        if (! isset($nodeIds[0])) {
            return null;
        }

        return $this->getMetaConnect((string) $nodeIds[0], $mode);
    }

    public function getDataConnect(string $key, int $mode = self::SOCKET_MODE_ASYNC): ?CommonSocket
    {
        return $this->getConnect($key, 'dataSockets', $mode);
    }

    public function getConnect(string $key, string $type, int $mode = self::SOCKET_MODE_ASYNC): ?CommonSocket
    {
        if (isset($this->{$type}[$key][$mode])) {
            return $this->{$type}[$key][$mode];
        }

        if (isset($this->brokers[$key])) {
            $hostname = $this->brokers[$key];
            if (isset($this->{$type}[$hostname][$mode])) {
                return $this->{$type}[$hostname][$mode];
            }
        }

        $host = null;
        $port = null;

        if (isset($this->brokers[$key])) {
            $hostname = $this->brokers[$key];

            [$host, $port] = explode(':', $hostname);
        }

        if (strpos($key, ':') !== false) {
            [$host, $port] = explode(':', $key);
        }

        if ($host === null || $port === null || ($mode === self::SOCKET_MODE_ASYNC && $this->process === null)) {
            return null;
        }

        try {
            $socket = $this->getSocket((string) $host, (int) $port, $mode);

            if ($socket instanceof Socket && $this->process !== null) {
                $socket->setOnReadable($this->process);
            }

            $socket->connect();
            $this->{$type}[$key][$mode] = $socket;

            return $socket;
        } catch (\Throwable $e) {
            $this->error($e->getMessage());
            return null;
        }
    }

    public function clear(): void
    {
        $sockets = [$this->metaSockets, $this->dataSockets];

        array_walk_recursive($sockets, function (CommonSocket $socket): void {
            $socket->close();
        });

        $this->brokers = [];
    }

    /**
     * @throws \Kafka\Exception
     */
    public function getSocket(string $host, int $port, int $mode): CommonSocket
    {
        $saslProvider = $this->judgeConnectionConfig();

        if ($mode === self::SOCKET_MODE_SYNC) {
            return $this->getSocketFactory()->createSocketSync($host, $port, $this->config, $saslProvider);
        }

        return $this->getSocketFactory()->createSocket($host, $port, $this->config, $saslProvider);
    }

    /**
     * @throws \Kafka\Exception
     */
    private function judgeConnectionConfig(): ?SaslMechanism
    {
        if ($this->config === null) {
            return null;
        }

        $plainConnections = [
            Config::SECURITY_PROTOCOL_PLAINTEXT,
            Config::SECURITY_PROTOCOL_SASL_PLAINTEXT,
        ];

        $saslConnections = [
            Config::SECURITY_PROTOCOL_SASL_SSL,
            Config::SECURITY_PROTOCOL_SASL_PLAINTEXT,
        ];

        $securityProtocol = $this->config->getSecurityProtocol();

        $this->config->setSslEnable(! in_array($securityProtocol, $plainConnections, true));

        if (in_array($securityProtocol, $saslConnections, true)) {
            return $this->getSaslMechanismProvider($this->config);
        }

        return null;
    }

    /**
     * @throws \Kafka\Exception
     */
    private function getSaslMechanismProvider(Config $config): SaslMechanism
    {
        $mechanism = $config->getSaslMechanism();
        $username  = $config->getSaslUsername();
        $password  = $config->getSaslPassword();

        switch ($mechanism) {
            case Config::SASL_MECHANISMS_PLAIN:
                return new Plain($username, $password);
            case Config::SASL_MECHANISMS_GSSAPI:
                return Gssapi::fromKeytab($config->getSaslKeytab(), $config->getSaslPrincipal());
            case Config::SASL_MECHANISMS_SCRAM_SHA_256:
                return new Scram($username, $password, Scram::SCRAM_SHA_256);
            case Config::SASL_MECHANISMS_SCRAM_SHA_512:
                return new Scram($username, $password, Scram::SCRAM_SHA_512);
        }

        throw new Exception(sprintf('"%s" is an invalid SASL mechanism', $mechanism));
    }

    private function getSocketFactory(): SocketFactory
    {
        if ($this->socketFactory === null) {
            $this->socketFactory = new SocketFactory();
        }

        return $this->socketFactory;
    }
}
