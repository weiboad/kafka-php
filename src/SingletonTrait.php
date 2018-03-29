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
     * Need to be compatible php 7.1.x, so this scene cannot be specified return type `object`
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
