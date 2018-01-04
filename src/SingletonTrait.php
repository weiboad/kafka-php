<?php
declare(strict_types=1);

namespace Kafka;

use Psr\Log\LoggerAwareTrait;

trait SingletonTrait
{
    use LoggerAwareTrait;
    use LoggerTrait;

    /**
     * @var object
     */
    protected static $instance;

    /**
     * @return object
     */
    public static function getInstance()
    {
        if (self::$instance === null) {
            static::$instance = new static();
        }

        return static::$instance;
    }

    private function __construct()
    {
    }
}
