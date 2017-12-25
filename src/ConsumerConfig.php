<?php
namespace Kafka;

/**
 * @method string|false ietGroupId()
 * @method array|false ietTopics()
 */
class ConsumerConfig extends Config
{
    use SingletonTrait;

    public const CONSUME_AFTER_COMMIT_OFFSET  = 1;
    public const CONSUME_BEFORE_COMMIT_OFFSET = 2;

    protected $runtimeOptions = [
        'consume_mode' => self::CONSUME_AFTER_COMMIT_OFFSET,
    ];

    protected static $defaults = [
        'groupId' => '',
        'sessionTimeout' => 30000,
        'rebalanceTimeout' => 30000,
        'topics' => [],
        'offsetReset' => 'latest', // earliest
        'maxBytes' => 65536, // 64kb
        'maxWaitTime' => 100,
    ];

    public function getGroupId()
    {
        $groupId = trim($this->ietGroupId());

        if ($groupId == false || $groupId == '') {
            throw new \Kafka\Exception\Config('Get group id value is invalid, must set it not empty string');
        }

        return $groupId;
    }

    public function setGroupId($groupId)
    {
        $groupId = trim($groupId);
        if ($groupId == false || $groupId == '') {
            throw new \Kafka\Exception\Config('Set group id value is invalid, must set it not empty string');
        }
        static::$options['groupId'] = $groupId;
    }

    public function setSessionTimeout($sessionTimeout)
    {
        if (! is_numeric($sessionTimeout) || $sessionTimeout < 1 || $sessionTimeout > 3600000) {
            throw new \Kafka\Exception\Config('Set session timeout value is invalid, must set it 1 .. 3600000');
        }
        static::$options['sessionTimeout'] = $sessionTimeout;
    }

    public function setRebalanceTimeout($rebalanceTimeout)
    {
        if (! is_numeric($rebalanceTimeout) || $rebalanceTimeout < 1 || $rebalanceTimeout > 3600000) {
            throw new \Kafka\Exception\Config('Set rebalance timeout value is invalid, must set it 1 .. 3600000');
        }
        static::$options['rebalanceTimeout'] = $rebalanceTimeout;
    }

    public function setOffsetReset($offsetReset)
    {
        if (! in_array($offsetReset, ['latest', 'earliest'])) {
            throw new \Kafka\Exception\Config('Set offset reset value is invalid, must set it `latest` or `earliest`');
        }
        static::$options['offsetReset'] = $offsetReset;
    }

    public function getTopics()
    {
        $topics = $this->ietTopics();

        if (empty($topics)) {
            throw new \Kafka\Exception\Config('Get consumer topics value is invalid, must set it not empty');
        }

        return $topics;
    }

    public function setTopics($topics)
    {
        if (! is_array($topics) || empty($topics)) {
            throw new \Kafka\Exception\Config('Set consumer topics value is invalid, must set it not empty array');
        }
        static::$options['topics'] = $topics;
    }

    public function setConsumeMode($mode)
    {
        $this->runtimeOptions['consume_mode'] = $mode;
    }

    public function getConsumeMode()
    {
        return $this->runtimeOptions['consume_mode'];
    }
}
