<?php
namespace Kafka;

class Loop
{
    use \Kafka\SingletonTrait;

    private $watchers = [];

    public function repeat($interval, callable $callback)
    {
        $watcherId = \Amp\repeat($callback, $interval);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function defer(callable $callback)
    {
        $watcherId = \Amp\immediately($callback);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function cancel($watcherId)
    {
        \Amp\cancel($watcherId);
        $this->delWatcher($watcherId);
    }

    public function disable($watcherId)
    {
        \Amp\disable($watcherId);
    }

    public function enable($watcherId)
    {
        \Amp\enable($watcherId);
    }

    public function delay($delay, callable $callback, $data = [])
    {
        $watcherId = \Amp\once($callback, $delay, $data);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function onReadable($stream, callable $callback, $data = [])
    {
        $watcherId = \Amp\onReadable($stream, $callback, $data);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function onWritable($stream, callable $callback, $data = [])
    {
        $watcherId = \Amp\onWritable($stream, $callback, $data);
        $this->addWatcher($watcherId);
        return $watcherId;
    }

    public function run()
    {
        $info = \Amp\info();
        if (isset($info['running']) && $info['running'] === true) {
            return;
        }

        \Amp\run();
    }

    public function stop()
    {
        \Amp\stop();
        foreach ($this->watchers as $watcherId => $unused) {
            $this->cancel($watcherId);
        }
    }

    public function getInfo()
    {
        return \Amp\info();
    }

    private function addWatcher($watcherId)
    {
        $this->watchers[$watcherId] = true;
    }

    private function delWatcher($watcherId)
    {
        if (! isset($this->watchers[$watcherId])) {
            return;
        }
        unset($this->watchers[$watcherId]);
    }
}
