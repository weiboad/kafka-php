<?php
namespace Kafka\Contracts\Config;

interface Producer
{
	public function getRequestTimeout() : int;
    public function getProduceInterval() : int;
    public function getTimeout() : int;
    public function getRequiredAck() : int;
}
