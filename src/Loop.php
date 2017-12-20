<?php
namespace Kafka;

use Amp\Loop as AmpLoop;

class Loop
{
    private $watchers = [];

    public function repeat(int $interval, callable $callback) : string
    {
        $watcherId = AmpLoop::repeat($interval, $callback);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function cancel(string $watcherId) : void
    {
        AmpLoop::cancel($watcherId);
        $this->delWatcher($watcherId);
    }

    public function disable(string $watcherId) : void
    {
        AmpLoop::disable($watcherId);
    }

    public function enable(string $watcherId) : void
    {
        AmpLoop::enable($watcherId);
    }

    public function delay(int $delay, callable $callback, $data = null) : string
    {
        $watcherId = AmpLoop::delay($delay, $callback, $data);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function onReadable($stream, callable $callback, $data = null) : string
    {
        $watcherId = AmpLoop::onReadable($stream, $callback, $data);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function onWritable($stream, callable $callback, $data = null) : string
    {
        $watcherId = AmpLoop::onWritable($stream, $callback, $data);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function run() : void
    {
        $info = AmpLoop::getInfo();
        if (isset($info['running']) && $info['running'] === true) {
            return;
        }

        AmpLoop::run();
    }

    public function stop() : void
    {
        AmpLoop::stop();
        foreach ($this->watchers as $watcherId => $unused) {
            $this->cancel($watcherId);
        }
    }

    public function getInfo() : array
    {
        return AmpLoop::getInfo();
    }

    private function addWatcher(string $watcherId) : void
    {
        $this->watchers[$watcherId] = true;
    }

    private function delWatcher(string $watcherId) : void
    {
        if (! isset($this->watchers[$watcherId])) {
            return;
        }
        unset($this->watchers[$watcherId]);
    }
}
