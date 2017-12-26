<?php
namespace Kafka;

trait SingletonTrait
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

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
