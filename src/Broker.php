<?php
namespace Kafka;

use DI\FactoryInterface;

use Kafka\Contracts\BrokerInterface;
use Kafka\Contracts\SocketInterface;
use Kafka\Exception;

class Broker implements BrokerInterface
{
    private $groupBrokerId = null;

    private $topics = [];

    private $brokers = [];

    private $metaSockets = [];

    private $dataSockets = [];

    private $process;

	private $container;

	public function __construct(FactoryInterface $container)
	{
		$this->container = $container;	
	}

    public function setProcess(callable $process) : void
    {
        $this->process = $process;
    }

    public function setGroupBrokerId($brokerId) : void
    {
        $this->groupBrokerId = $brokerId;
    }

    public function getGroupBrokerId() : int
    {
        return $this->groupBrokerId;
    }

    public function setData(array $topics, array $brokersResult) : bool
    {
        $brokers = [];
        foreach ($brokersResult as $value) {
            $key           = $value['nodeId'];
            $hostname      = $value['host'] . ':' . $value['port'];
            $brokers[$key] = $hostname;
        }

        $change = false;
        if (serialize($this->brokers) != serialize($brokers)) {
            $this->brokers = $brokers;
            $change        = true;
        }

        $newTopics = [];
        foreach ($topics as $topic) {
            if ($topic['errorCode'] != \Kafka\Protocol::NO_ERROR) {
                $this->error('Parse metadata for topic is error, error:' . \Kafka\Protocol::getError($topic['errorCode']));
                continue;
            }
            $item = [];
            foreach ($topic['partitions'] as $part) {
                $item[$part['partitionId']] = $part['leader'];
            }
            $newTopics[$topic['topicName']] = $item;
        }

        if (serialize($this->topics) != serialize($newTopics)) {
            $this->topics = $newTopics;
            $change       = true;
        }

        return $change;
    }

    public function getTopics() : array
    {
        return $this->topics;
    }

    public function getBrokers() : array
    {
        return $this->brokers;
    }

    public function getMetaConnect($key) : SocketInterface
    {
        return $this->getConnect($key, 'metaSockets');
    }

    public function getRandConnect() : SocketInterface
    {
        $nodeIds = array_keys($this->brokers);
        shuffle($nodeIds);
        if (! isset($nodeIds[0])) {
			throw new Exception('Invalid broker list, must call in after setData');
        }
        return $this->getMetaConnect($nodeIds[0]);
    }

    public function getDataConnect($key) : SocketInterface
    {
        return $this->getConnect($key, 'dataSockets');
    }

    private function getConnect($key, $type) : SocketInterface
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
            $hostname          = $this->brokers[$key];
            list($host, $port) = explode(':', $hostname);
        }

        if (strpos($key, ':') !== false) {
            list($host, $port) = explode(':', $key);
        }

        if (! $host || ! $port) {
			throw new Exception('Invalid broker list, must give host and port');
        }

		$socket = $this->getSocket($host, $port);
		$socket->setOnReadable($this->process);
		$socket->connect();
		$this->{$type}[$key] = $socket;

		return $socket;
    }

    public function clear() : void
    {
        foreach ($this->metaSockets as $key => $socket) {
            $socket->close();
        }
        foreach ($this->dataSockets as $key => $socket) {
            $socket->close();
        }
        $this->brokers = [];
    }

    private function getSocket($host, $port) : SocketInterface
    {
		return $this->container->make(SocketInterface::class, [
			'host' => $host,
			'port' => $port,
		]);
    }
}
