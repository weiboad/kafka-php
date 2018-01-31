<?php
namespace Kafka\Consumer;

use Kafka\Consumer;

interface StopStrategy
{
    public function setup(Consumer $consumer);
}
