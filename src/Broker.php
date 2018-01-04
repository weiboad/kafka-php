<?php
declare(strict_types=1);

namespace Kafka;

use Kafka\Sasl\Gssapi;
use Kafka\Sasl\Plain;
use Kafka\Sasl\Scram;

class Broker
{
    use SingletonTrait;

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

    /**
     * @var callable|null
     */
    private $process;

    /**
     * @var Config|null
     */
    private $config;

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
     * @param mixed[] $brokersResult
     */
    public function setData(array $topics, array $brokersResult): bool
    {
        $brokers = [];

        foreach ($brokersResult as $value) {
            $brokers[$value['nodeId']] = $value['host'] . ':' . $value['port'];
        }

        $changed = false;

        if (\serialize($this->brokers) !== \serialize($brokers)) {
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

        if (\serialize($this->topics) !== \serialize($newTopics)) {
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

    public function getMetaConnect(string $key, bool $modeSync = false): ?CommonSocket
    {
        return $this->getConnect($key, 'metaSockets', $modeSync);
    }

    public function getRandConnect(bool $modeSync = false): ?CommonSocket
    {
        $nodeIds = \array_keys($this->brokers);
        \shuffle($nodeIds);

        if (! isset($nodeIds[0])) {
            return null;
        }

        return $this->getMetaConnect((string) $nodeIds[0], $modeSync);
    }

    public function getDataConnect(string $key, bool $modeSync = false): ?CommonSocket
    {
        return $this->getConnect($key, 'dataSockets', $modeSync);
    }

    public function getConnect(string $key, string $type, bool $modeSync = false): ?CommonSocket
    {
        if (isset($this->{$type}[$key])) {
            return $this->{$type}[$key];
        }

        if (isset($this->brokers[$key])) {
            $hostname = $this->brokers[$key];
            if (isset($this->{$type}[$hostname])) {
                return $this->{$type}[$hostname];
            }
        }

        $host = null;
        $port = null;

        if (isset($this->brokers[$key])) {
            $hostname = $this->brokers[$key];

            [$host, $port] = \explode(':', $hostname);
        }

        if (\strpos($key, ':') !== false) {
            [$host, $port] = \explode(':', $key);
        }

        if ($host === null || $port === null || (! $modeSync && $this->process === null)) {
            return null;
        }

        try {
            $socket = $this->getSocket((string) $host, (int) $port, $modeSync);

            if ($socket instanceof Socket && $this->process !== null) {
                $socket->setOnReadable($this->process);
            }

            $socket->connect();
            $this->{$type}[$key] = $socket;

            return $socket;
        } catch (\Throwable $e) {
            $this->error($e->getMessage());
            return null;
        }
    }

    public function clear(): void
    {
        foreach ($this->metaSockets as $key => $socket) {
            $socket->close();
        }
        foreach ($this->dataSockets as $key => $socket) {
            $socket->close();
        }
        $this->brokers = [];
    }

    /**
     * @throws \Kafka\Exception
     */
    public function getSocket(string $host, int $port, bool $modeSync): CommonSocket
    {
        $saslProvider = $this->judgeConnectionConfig();

        if ($modeSync) {
            return new SocketSync($host, $port, $this->config, $saslProvider);
        }

        return new Socket($host, $port, $this->config, $saslProvider);
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

        $this->config->setSslEnable(! \in_array($securityProtocol, $plainConnections, true));

        if (\in_array($securityProtocol, $saslConnections, true)) {
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

        throw new Exception(\sprintf('"%s" is an invalid SASL mechanism', $mechanism));
    }
}
