<?php
namespace Kafka;

use Amp as AmpLoop;

class Loop
{
	use \Kafka\SingletonTrait;

    private $watchers = [];

    public function repeat(int $interval, callable $callback) 
    {
        $watcherId = AmpLoop::repeat($interval, $callback);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function defer(callable $callback) 
    {
        $watcherId = AmpLoop::defer($callback);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function cancel(string $watcherId) 
    {
        AmpLoop::cancel($watcherId);
        $this->delWatcher($watcherId);
    }

    public function disable(string $watcherId) 
    {
        AmpLoop::disable($watcherId);
    }

    public function enable(string $watcherId) 
    {
        AmpLoop::enable($watcherId);
    }

    public function delay(int $delay, callable $callback, $data = null) 
    {
        $watcherId = AmpLoop::delay($delay, $callback, $data);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function onReadable($stream, callable $callback, $data = null) 
    {
        $watcherId = AmpLoop::onReadable($stream, $callback, $data);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function onWritable($stream, callable $callback, $data = null) 
    {
        $watcherId = AmpLoop::onWritable($stream, $callback, $data);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function run() 
    {
        $info = AmpLoop::getInfo();
        if (isset($info['running']) && $info['running'] === true) {
            return;
        }

        AmpLoop::run();
    }

    public function stop() 
    {
        AmpLoop::stop();
        foreach ($this->watchers as $watcherId => $unused) {
            $this->cancel($watcherId);
        }
    }

    public function getInfo() 
    {
        return AmpLoop::getInfo();
    }

    private function addWatcher(string $watcherId) 
    {
        $this->watchers[$watcherId] = true;
    }

    private function delWatcher(string $watcherId) 
    {
        if (! isset($this->watchers[$watcherId])) {
            return;
        }
        unset($this->watchers[$watcherId]);
    }
}
