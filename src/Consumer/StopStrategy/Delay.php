<?php
namespace Kafka\Consumer\StopStrategy;

use Kafka\Loop;
use Kafka\Consumer;
use Kafka\Consumer\StopStrategy;

final class Delay implements StopStrategy
{
    /**
     * The amount of time, in milliseconds, to stop the consumer
     *
     * @var int
     */
    private $delay;
    private $loop;

    public function __construct($delay)
    {
        $this->delay = $delay;
        $this->loop  = Loop::getInstance();
    }

    public function setup(Consumer $consumer)
    {
        $this->loop->delay(
            $this->delay,
            function () use ($consumer) {
                $consumer->stop();
            }
        );
    }
}
