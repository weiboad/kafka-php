<?php
namespace Kafka\Contracts\Consumer;

use Kafka\Consumer;

interface State
{
    public function init(): void;
    public function start(): void;
    public function stop(): void;
	public function succRun(int $key, $context = null) : void;
	public function failRun(int $key, $context = null) : void;
	public function setCallback($callbacks);
	public function rejoin();
	public function recover();
}
