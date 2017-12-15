<?php
namespace Kafka\Config;

use Kafka\Exception;
use Kafka\Contracts\Config\Consumer as ConsumerInterface;

class Consumer implements ConsumerInterface
{
    const CONSUME_AFTER_COMMIT_OFFSET  = 1;
    const CONSUME_BEFORE_COMMIT_OFFSET = 2;

	private $groupId = '';
	private $sessionTimeout = 30000;
	private $rebalanceTimeout = 30000;
	private $topics = [];
	private $offsetReset = 'latest';
	private $maxBytes = 65536;
	private $maxWaitTime = 100;
	private $consumeMode = self::CONSUME_AFTER_COMMIT_OFFSET;

    public function getGroupId() : string
    {
        if ($this->groupId == '') {
            throw new \Kafka\Exception\Config('Get group id value is invalid, must set it not empty string');
        }

        return $this->groupId;
    }

	public function getSessionTimeout() : int
	{
		return $this->sessionTimeout;	
	}

	public function getRebalanceTimeout() : int
	{
		return $this->rebalanceTimeout;	
	}

    public function getTopics() : array
    {
        if (empty($this->topics)) {
            throw new \Kafka\Exception\Config('Get consumer topics value is invalid, must set it not empty');
        }

        return $this->topics;
    }

	public function getOffsetReset() : string
	{
		return $this->offsetReset;	
	}

	public function getMaxBytes() : int
	{
		return $this->maxBytes;	
	}

	public function getMaxWaitTime() : int
	{
		return $this->maxWaitTime;	
	}

    public function getConsumeMode() : int
    {
		return $this->consumeMode;
    }

    public function setGroupId(string $groupId) : void
    {
        $groupId = trim($groupId);
        if ($groupId == '') {
            throw new \Kafka\Exception\Config('Set group id value is invalid, must set it not empty string');
        }
		$this->groupId = $groupId;
    }

    public function setSessionTimeout(int $sessionTimeout) : void
    {
        if (! is_numeric($sessionTimeout) || $sessionTimeout < 1 || $sessionTimeout > 3600000) {
            throw new \Kafka\Exception\Config('Set session timeout value is invalid, must set it 1 .. 3600000');
        }
		$this->sessionTimeout = $sessionTimeout;
    }

    public function setRebalanceTimeout(int $rebalanceTimeout) : void
    {
        if (! is_numeric($rebalanceTimeout) || $rebalanceTimeout < 1 || $rebalanceTimeout > 3600000) {
            throw new \Kafka\Exception\Config('Set rebalance timeout value is invalid, must set it 1 .. 3600000');
        }
		$this->rebalanceTimeout = $rebalanceTimeout;
    }

    public function setOffsetReset(string $offsetReset) : void
    {
        if (! in_array($offsetReset, ['latest', 'earliest'])) {
            throw new \Kafka\Exception\Config('Set offset reset value is invalid, must set it `latest` or `earliest`');
        }
		$this->offsetReset = $offsetReset;
    }

    public function setTopics(array $topics) : void
    {
        if (! is_array($topics) || empty($topics)) {
            throw new \Kafka\Exception\Config('Set consumer topics value is invalid, must set it not empty array');
        }
		$this->topics = $topics;
    }

    public function setMaxBytes(int $maxBytes) : void
	{
		$this->maxBytes = $maxBytes;
	}

    public function setMaxWaitTime(int $maxWaitTime) : void
	{
		$this->maxWaitTime = $maxWaitTime;
	}

    public function setConsumeMode(int $mode) : void
    {
		$this->consumeMode = $mode;
    }
}
