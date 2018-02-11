<?php
declare(strict_types=1);

namespace Kafka;

use function in_array;
use function trim;

/**
 * @method string|false ietGroupId()
 * @method array|false ietTopics()
 * @method int getSessionTimeout()
 * @method int getRebalanceTimeout()
 * @method string getOffsetReset()
 * @method int getMaxBytes()
 * @method int getMaxWaitTime()
 */
class ConsumerConfig extends Config
{
    private const CONSUME_MODE = 'consume_mode';
    use SingletonTrait;

    /** @var mixed[] */
    private $runtimeOptions = [];

    /**
     * @var mixed[]
     */
    protected static $defaults = [
        'groupId'          => '',
        'sessionTimeout'   => 30000,
        'rebalanceTimeout' => 30000,
        'topics'           => [],
        'offsetReset'      => 'latest', // earliest
        'maxBytes'         => 65536, // 64kb
        'maxWaitTime'      => 100,
    ];

    private function __construct()
    {
        $this->runtimeOptions[self::CONSUME_MODE] = ConsumeMode::consumeAfterCommitOffset();
    }

    /**
     * @throws \Kafka\Exception\Config
     */
    public function getGroupId(): string
    {
        $groupId = trim($this->ietGroupId());

        if ($groupId === false || $groupId === '') {
            throw new Exception\Config('Get group id value is invalid, must set it not empty string');
        }

        return $groupId;
    }

    /**
     * @throws \Kafka\Exception\Config
     */
    public function setGroupId(string $groupId): void
    {
        $groupId = trim($groupId);

        if ($groupId === false || $groupId === '') {
            throw new Exception\Config('Set group id value is invalid, must set it not empty string');
        }

        static::$options['groupId'] = $groupId;
    }

    /**
     * @throws \Kafka\Exception\Config
     */
    public function setSessionTimeout(int $sessionTimeout): void
    {
        if ($sessionTimeout < 1 || $sessionTimeout > 3600000) {
            throw new Exception\Config('Set session timeout value is invalid, must set it 1 .. 3600000');
        }

        static::$options['sessionTimeout'] = $sessionTimeout;
    }

    /**
     * @throws \Kafka\Exception\Config
     */
    public function setRebalanceTimeout(int $rebalanceTimeout): void
    {
        if ($rebalanceTimeout < 1 || $rebalanceTimeout > 3600000) {
            throw new Exception\Config('Set rebalance timeout value is invalid, must set it 1 .. 3600000');
        }

        static::$options['rebalanceTimeout'] = $rebalanceTimeout;
    }

    /**
     * @throws \Kafka\Exception\Config
     */
    public function setOffsetReset(string $offsetReset): void
    {
        if (! in_array($offsetReset, ['latest', 'earliest'], true)) {
            throw new Exception\Config('Set offset reset value is invalid, must set it `latest` or `earliest`');
        }

        static::$options['offsetReset'] = $offsetReset;
    }

    /**
     * @return string[]
     *
     * @throws \Kafka\Exception\Config
     */
    public function getTopics(): array
    {
        $topics = $this->ietTopics();

        if (empty($topics)) {
            throw new Exception\Config('Get consumer topics value is invalid, must set it not empty');
        }

        return $topics;
    }

    /**
     * @param string[] $topics
     *
     * @throws \Kafka\Exception\Config
     */
    public function setTopics(array $topics): void
    {
        if (empty($topics)) {
            throw new Exception\Config('Set consumer topics value is invalid, must set it not empty array');
        }

        static::$options['topics'] = $topics;
    }

    public function setConsumeMode(ConsumeMode $consumeMode): void
    {
        $this->runtimeOptions[self::CONSUME_MODE] = $consumeMode;
    }

    public function getConsumeMode(): ConsumeMode
    {
        return $this->runtimeOptions[self::CONSUME_MODE];
    }
}
