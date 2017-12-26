<?php
namespace Kafka;

use Psr\Log\LoggerAwareTrait;

trait SingletonTrait
{
    use LoggerAwareTrait;
    use LoggerTrait;

    protected static $instance;

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
