<?php
namespace Kafka\Contracts\Config;

interface Socket 
{
	public function getSendTimeoutSec() : int;
	public function getSendTimeoutUsec() : int;
	public function getRecvTimeoutSec() : int;
	public function getRecvTimeoutUsec() : int;
	public function getMaxWriteAttempts() : int;
}
