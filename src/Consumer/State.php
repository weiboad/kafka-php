<?php
namespace Kafka\Consumer;

use Kafka\Loop;

class State
{
    use \Kafka\SingletonTrait;

    const REQUEST_METADATA      = 1;
    const REQUEST_GETGROUP      = 2;
    const REQUEST_JOINGROUP     = 3;
    const REQUEST_SYNCGROUP     = 4;
    const REQUEST_HEARTGROUP    = 5;
    const REQUEST_OFFSET        = 6;
    const REQUEST_FETCH         = 7;
    const REQUEST_FETCH_OFFSET  = 8;
    const REQUEST_COMMIT_OFFSET = 9;

    const STATUS_INIT    = 0;
    const STATUS_STOP    = 1;
    const STATUS_START   = 2;
    const STATUS_LOOP    = 4;
    const STATUS_PROCESS = 8;
    const STATUS_FINISH  = 16;

    private static $cleanRequestState = [
        self::REQUEST_METADATA      => [],
        self::REQUEST_GETGROUP      => [],
        self::REQUEST_JOINGROUP     => [],
        self::REQUEST_SYNCGROUP     => [],
        self::REQUEST_HEARTGROUP    => [],
        self::REQUEST_OFFSET        => ['interval' => 2000],
        self::REQUEST_FETCH         => ['interval' => 100],
        self::REQUEST_FETCH_OFFSET  => ['interval' => 2000],
        self::REQUEST_COMMIT_OFFSET => ['norepeat' => true],
    ];

    private $callStatus = [];

    private $requests = [
        self::REQUEST_METADATA      => [],
        self::REQUEST_GETGROUP      => [],
        self::REQUEST_JOINGROUP     => [],
        self::REQUEST_SYNCGROUP     => [],
        self::REQUEST_HEARTGROUP    => [],
        self::REQUEST_OFFSET        => ['interval' => 2000],
        self::REQUEST_FETCH         => ['interval' => 100],
        self::REQUEST_FETCH_OFFSET  => ['interval' => 2000],
        self::REQUEST_COMMIT_OFFSET => ['norepeat' => true],
    ];

    private $loop = null;

    public function init()
    {
        $this->loop       = Loop::getInstance();
        $this->callStatus = [
            self::REQUEST_METADATA      => ['status' => self::STATUS_LOOP],
            self::REQUEST_GETGROUP      => ['status' => self::STATUS_START],
            self::REQUEST_JOINGROUP     => ['status' => self::STATUS_START],
            self::REQUEST_SYNCGROUP     => ['status' => self::STATUS_START],
            self::REQUEST_HEARTGROUP    => ['status' => self::STATUS_LOOP],
            self::REQUEST_OFFSET        => ['status' => self::STATUS_LOOP],
            self::REQUEST_FETCH         => ['status' => self::STATUS_LOOP],
            self::REQUEST_FETCH_OFFSET  => ['status' => self::STATUS_LOOP],
            self::REQUEST_COMMIT_OFFSET => ['status' => self::STATUS_LOOP],
        ];

        // instances clear

        // init requests
        $config = \Kafka\lib\ConsumerConfig::getInstance();
        foreach ($this->requests as $request => $option) {
            switch ($request) {
                case self::REQUEST_METADATA:
                    $this->requests[$request]['interval'] = $config->getMetadataRefreshIntervalMs();
                    break;
                default:
                    $this->requests[$request]['interval'] = 1000;
            }
        }
    }

    public function start()
    {
        foreach ($this->requests as $request => $option) {
            if (isset($option['norepeat']) && $option['norepeat']) {
                continue;
            }
            $interval = isset($option['interval']) ? $option['interval'] : 200;
            $this->loop->repeat($interval, function ($watcherId) use ($request, $option) {
                if ($this->checkRun($request) && $option['func'] != null) {
                    $context = call_user_func($option['func']);
                    $this->processing($request, $context);
                }
                $this->requests[$request]['watcher'] = $watcherId;
            });
        }

        // start sync metadata
        if (isset($request, $this->requests[self::REQUEST_METADATA]['func'])) {
            $context = call_user_func($this->requests[self::REQUEST_METADATA]['func']);
            $this->processing($request, $context);
        }
        $this->loop->repeat(1000, function ($watcherId) {
            $this->report();
        });
    }

    public function stop()
    {
        $this->removeWatchers();

        $this->callStatus = [];
        $this->requests   = self::$cleanRequestState;
    }

    private function removeWatchers()
    {
        foreach (array_keys($this->requests) as $request) {
            if ($this->requests[$request]['watcher'] === null) {
                return;
            }

            $this->loop->cancel($this->requests[$request]['watcher']);
        }
    }

    public function succRun($key, $context = null)
    {
        if (! isset($this->callStatus[$key])) {
            return false;
        }

        switch ($key) {
            case self::REQUEST_METADATA:
                $this->callStatus[$key]['status'] = (self::STATUS_LOOP | self::STATUS_FINISH);
                if ($context) { // if kafka broker is change
                    $this->recover();
                }
                break;
            case self::REQUEST_GETGROUP:
            case self::REQUEST_JOINGROUP:
            case self::REQUEST_SYNCGROUP:
                $this->callStatus[$key]['status'] = (self::STATUS_STOP | self::STATUS_FINISH);
                break;
            case self::REQUEST_HEARTGROUP:
            case self::REQUEST_FETCH_OFFSET:
            case self::REQUEST_COMMIT_OFFSET:
                $this->callStatus[$key]['status'] = (self::STATUS_LOOP | self::STATUS_FINISH);
                break;
            case self::REQUEST_OFFSET:
                if (! isset($this->callStatus[$key]['context'])) {
                    $this->callStatus[$key]['status'] = (self::STATUS_LOOP | self::STATUS_FINISH);
                    break;
                }
                unset($this->callStatus[$key]['context'][$context]);
                $contextStatus = $this->callStatus[$key]['context'];
                if (empty($contextStatus)) {
                    $this->callStatus[$key]['status'] = (self::STATUS_LOOP | self::STATUS_FINISH);
                }
                break;
            case self::REQUEST_FETCH:
                if (! isset($this->callStatus[$key]['context'])) {
                    $this->callStatus[$key]['status'] = (self::STATUS_LOOP | self::STATUS_FINISH);
                    break;
                }
                unset($this->callStatus[$key]['context'][$context]);
                $contextStatus = $this->callStatus[$key]['context'];
                if (empty($contextStatus)) {
                    $this->callStatus[$key]['status'] = (self::STATUS_LOOP | self::STATUS_FINISH);
                    call_user_func($this->requests[self::REQUEST_COMMIT_OFFSET]['func']);
                }
                break;
        }
    }

    public function failRun($key, $context = null)
    {
        if (! isset($this->callStatus[$key])) {
            return false;
        }

        switch ($key) {
            case self::REQUEST_METADATA:
                $this->callStatus[$key]['status'] = self::STATUS_LOOP;
                break;
            case self::REQUEST_GETGROUP:
            case self::REQUEST_JOINGROUP:
            case self::REQUEST_SYNCGROUP:
                $this->recover();
                break;
        }
    }

    public function setCallback($callbacks)
    {
        foreach ($callbacks as $request => $callback) {
            $this->requests[$request]['func'] = $callback;
        }
    }

    public function rejoin()
    {
        $joinGroupStatus = $this->callStatus[self::REQUEST_JOINGROUP]['status'];
        if (($joinGroupStatus & self::STATUS_PROCESS) == self::STATUS_PROCESS) {
            return;
        }

        $this->callStatus = [
            self::REQUEST_METADATA      => $this->callStatus[self::REQUEST_METADATA],
            self::REQUEST_GETGROUP      => $this->callStatus[self::REQUEST_GETGROUP],
            self::REQUEST_JOINGROUP     => ['status' => self::STATUS_START],
            self::REQUEST_SYNCGROUP     => ['status' => self::STATUS_START],
            self::REQUEST_HEARTGROUP    => ['status' => self::STATUS_LOOP],
            self::REQUEST_OFFSET        => ['status' => self::STATUS_LOOP],
            self::REQUEST_FETCH         => ['status' => self::STATUS_LOOP],
            self::REQUEST_FETCH_OFFSET  => ['status' => self::STATUS_LOOP],
            self::REQUEST_COMMIT_OFFSET => ['status' => self::STATUS_LOOP],
        ];
    }

    public function recover()
    {
        $this->callStatus = [
            self::REQUEST_METADATA      => $this->callStatus[self::REQUEST_METADATA],
            self::REQUEST_GETGROUP      => ['status' => self::STATUS_START],
            self::REQUEST_JOINGROUP     => ['status' => self::STATUS_START],
            self::REQUEST_SYNCGROUP     => ['status' => self::STATUS_START],
            self::REQUEST_HEARTGROUP    => ['status' => self::STATUS_LOOP],
            self::REQUEST_OFFSET        => ['status' => self::STATUS_LOOP],
            self::REQUEST_FETCH         => ['status' => self::STATUS_LOOP],
            self::REQUEST_FETCH_OFFSET  => ['status' => self::STATUS_LOOP],
            self::REQUEST_COMMIT_OFFSET => ['status' => self::STATUS_LOOP],
        ];
    }

    protected function checkRun($key)
    {
        if (! isset($this->callStatus[$key])) {
            return false;
        }

        $status = $this->callStatus[$key]['status'];
        switch ($key) {
            case self::REQUEST_METADATA:
                if (($status & self::STATUS_PROCESS) == self::STATUS_PROCESS) {
                    return false;
                }
                if (($status & self::STATUS_LOOP) == self::STATUS_LOOP) {
                    return true;
                }
                return false;
            case self::REQUEST_GETGROUP:
                if (($status & self::STATUS_PROCESS) == self::STATUS_PROCESS) {
                    return false;
                }
                $metaStatus = $this->callStatus[self::REQUEST_METADATA]['status'];
                if (($metaStatus & self::STATUS_FINISH) != self::STATUS_FINISH) {
                    return false;
                }
                if (($status & self::STATUS_START) == self::STATUS_START) {
                    return true;
                }
                return false;
            case self::REQUEST_JOINGROUP:
                if (($status & self::STATUS_PROCESS) == self::STATUS_PROCESS) {
                    return false;
                }
                $groupStatus = $this->callStatus[self::REQUEST_GETGROUP]['status'];
                if (($groupStatus & self::STATUS_FINISH) != self::STATUS_FINISH) {
                    return false;
                }
                if (($status & self::STATUS_START) == self::STATUS_START) {
                    return true;
                }
                return false;
            case self::REQUEST_SYNCGROUP:
                if (($status & self::STATUS_PROCESS) == self::STATUS_PROCESS) {
                    return false;
                }
                $joinStatus = $this->callStatus[self::REQUEST_JOINGROUP]['status'];
                if (($joinStatus & self::STATUS_FINISH) != self::STATUS_FINISH) {
                    return false;
                }
                if (($status & self::STATUS_START) == self::STATUS_START) {
                    return true;
                }
                return false;
            case self::REQUEST_HEARTGROUP:
            case self::REQUEST_OFFSET:
                if (($status & self::STATUS_PROCESS) == self::STATUS_PROCESS) {
                    return false;
                }
                $syncStatus = $this->callStatus[self::REQUEST_SYNCGROUP]['status'];
                if (($syncStatus & self::STATUS_FINISH) != self::STATUS_FINISH) {
                    return false;
                }
                if (($status & self::STATUS_LOOP) == self::STATUS_LOOP) {
                    return true;
                }
                return false;
            case self::REQUEST_FETCH_OFFSET:
                if (($status & self::STATUS_PROCESS) == self::STATUS_PROCESS) {
                    return false;
                }
                $syncStatus = $this->callStatus[self::REQUEST_SYNCGROUP]['status'];
                if (($syncStatus & self::STATUS_FINISH) != self::STATUS_FINISH) {
                    return false;
                }
                $offsetStatus = $this->callStatus[self::REQUEST_OFFSET]['status'];
                if (($offsetStatus & self::STATUS_FINISH) != self::STATUS_FINISH) {
                    return false;
                }
                if (($status & self::STATUS_LOOP) == self::STATUS_LOOP) {
                    return true;
                }
                return false;
            case self::REQUEST_FETCH:
                if (($status & self::STATUS_PROCESS) == self::STATUS_PROCESS) {
                    return false;
                }
                $fetchOffsetStatus = $this->callStatus[self::REQUEST_FETCH_OFFSET]['status'];
                if (($fetchOffsetStatus & self::STATUS_FINISH) != self::STATUS_FINISH) {
                    return false;
                }
                $commitOffsetStatus = $this->callStatus[self::REQUEST_COMMIT_OFFSET]['status'];
                if (($commitOffsetStatus & self::STATUS_PROCESS) == self::STATUS_PROCESS) {
                    return false;
                }
                if (($status & self::STATUS_LOOP) == self::STATUS_LOOP) {
                    return true;
                }
                return false;
        }
    }

    protected function processing($key, $context)
    {
        if (! isset($this->callStatus[$key])) {
            return false;
        }

        // set process start time
        $this->callStatus[$key]['time'] = microtime(true);
        switch ($key) {
            case self::REQUEST_METADATA:
            case self::REQUEST_GETGROUP:
            case self::REQUEST_JOINGROUP:
            case self::REQUEST_SYNCGROUP:
            case self::REQUEST_HEARTGROUP:
            case self::REQUEST_FETCH_OFFSET:
            case self::REQUEST_COMMIT_OFFSET:
                $this->callStatus[$key]['status'] |= self::STATUS_PROCESS;
                break;
            case self::REQUEST_OFFSET:
            case self::REQUEST_FETCH:
                $this->callStatus[$key]['status'] |= self::STATUS_PROCESS;
                $contextStatus                     = [];
                foreach ($context as $fd) {
                    $contextStatus[$fd] = self::STATUS_PROCESS;
                }
                $this->callStatus[$key]['context'] = $contextStatus;
                break;
        }
    }

    protected function report()
    {
        //var_dump($this->callStatus[self::REQUEST_COMMIT_OFFSET]);
    }
}
