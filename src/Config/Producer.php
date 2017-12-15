<?php
namespace Kafka\Config;

use Kafka\Exception;
use Kafka\Contracts\Config\Producer as ProducerInterface;

class Producer implements ProducerInterface
{
	private $requiredAck = 1;
	private $timeout = 5000;
	private $requestTimeout = 6000;
	private $produceInterval = 100;

	public function getRequestTimeout() : int
	{
		return $this->requestTimeout;
	}

	public function getProduceInterval() : int
	{
		return $this->produceInterval;	
	}

	public function getTimeout() : int
	{
		return $this->timeout;
	}

	public function getRequiredAck() : int
	{
		return $this->requiredAck;	
	}

    public function setRequestTimeout(int $requestTimeout) : void
    {
        if (! is_numeric($requestTimeout) || $requestTimeout < 1 || $requestTimeout > 900000) {
            throw new \Kafka\Exception\Config('Set request timeout value is invalid, must set it 1 .. 900000');
        }
		$this->requestTimeout = $requestTimeout;
    }

    public function setProduceInterval(int $produceInterval) : void
    {
        if (! is_numeric($produceInterval) || $produceInterval < 1 || $produceInterval > 900000) {
            throw new \Kafka\Exception\Config('Set produce interval timeout value is invalid, must set it 1 .. 900000');
        }
		$this->produceInterval = $produceInterval;
    }

    public function setTimeout(int $timeout) : void
    {
        if (! is_numeric($timeout) || $timeout < 1 || $timeout > 900000) {
            throw new \Kafka\Exception\Config('Set timeout value is invalid, must set it 1 .. 900000');
        }
		$this->timeout = $timeout;
    }

    public function setRequiredAck(int $requiredAck) : void
    {
        if (! is_numeric($requiredAck) || $requiredAck < -1 || $requiredAck > 1000) {
            throw new \Kafka\Exception\Config('Set required ack value is invalid, must set it -1 .. 1000');
        }
		$this->requiredAck = $requiredAck;
    }
}
