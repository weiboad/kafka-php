<?php
namespace Kafka\Producer;

use Amp\Loop;

class State
{
    use \Kafka\SingletonTrait;

    public const REQUEST_METADATA = 1;
    public const REQUEST_PRODUCE  = 2;

    public const STATUS_INIT    = 0;
    public const STATUS_STOP    = 1;
    public const STATUS_START   = 2;
    public const STATUS_LOOP    = 4;
    public const STATUS_PROCESS = 8;
    public const STATUS_FINISH  = 16;

    private $callStatus = [];

    private $requests = [
        self::REQUEST_METADATA => [],
        self::REQUEST_PRODUCE => [],
    ];

    public function init()
    {
        $this->callStatus = [
            self::REQUEST_METADATA => [
                'status'=> self::STATUS_LOOP,
            ],
            self::REQUEST_PRODUCE => [
                'status'=> self::STATUS_LOOP,
            ],
        ];

        // instances clear

        // init requests
        $config = \Kafka\ConsumerConfig::getInstance();

        foreach ($this->requests as $request => $option) {
            switch ($request) {
                case self::REQUEST_METADATA:
                    $this->requests[$request]['interval'] = $config->getMetadataRefreshIntervalMs();
                    break;
                default:
                    $interval = $config->getIsAsyn() ? $config->getProduceInterval() : 1;

                    $this->requests[$request]['interval'] = $interval;
            }
        }
    }

    public function start()
    {
        foreach ($this->requests as $request => $option) {
            $interval = isset($option['interval']) ? $option['interval'] : 200;
            Loop::repeat($interval, function ($watcherId) use ($request, $option) {
                if ($this->checkRun($request) && $option['func'] !== null) {
                    $this->processing($request, $option['func']());
                }

                $this->requests[$request]['watcher'] = $watcherId;
            });
        }

        // start sync metadata
        if (isset($this->requests[self::REQUEST_METADATA]['func'])
            && $this->callStatus[self::REQUEST_METADATA]['status'] === self::STATUS_LOOP) {
            $context = $this->requests[self::REQUEST_METADATA]['func']();
            $this->processing($request, $context);
        }

        Loop::repeat(1000, function ($watcherId) {
            $this->report();
        });
    }

    public function succRun($key, $context = null)
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $isAsyn = $config->getIsAsyn();

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
            case self::REQUEST_PRODUCE:
                if ($context === null) {
                    if (! $isAsyn) {
                        $this->callStatus[$key]['status'] = self::STATUS_FINISH;
                        Loop::stop();
                    } else {
                        $this->callStatus[$key]['status'] = (self::STATUS_LOOP | self::STATUS_FINISH);
                    }
                    break;
                }
                unset($this->callStatus[$key]['context'][$context]);
                $contextStatus = $this->callStatus[$key]['context'];

                if (empty($contextStatus)) {
                    if (! $isAsyn) {
                        $this->callStatus[$key]['status'] = self::STATUS_FINISH;
                        Loop::stop();
                    } else {
                        $this->callStatus[$key]['status'] = (self::STATUS_LOOP | self::STATUS_FINISH);
                    }
                }
                break;
        }
    }

    public function failRun($key, $context = null): void
    {
        if (! isset($this->callStatus[$key])) {
            return;
        }

        switch ($key) {
            case self::REQUEST_METADATA:
                $this->callStatus[$key]['status'] = self::STATUS_LOOP;
                break;
            case self::REQUEST_PRODUCE:
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

    public function recover()
    {
        $this->callStatus = [
            self::REQUEST_METADATA => $this->callStatus[self::REQUEST_METADATA],
            self::REQUEST_PRODUCE => [
                'status'=> self::STATUS_LOOP,
            ],
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
                if (($status & self::STATUS_PROCESS) === self::STATUS_PROCESS) {
                    return false;
                }

                if (($status & self::STATUS_LOOP) === self::STATUS_LOOP) {
                    return true;
                }

                return false;
            case self::REQUEST_PRODUCE:
                if (($status & self::STATUS_PROCESS) === self::STATUS_PROCESS) {
                    return false;
                }

                $syncStatus = $this->callStatus[self::REQUEST_METADATA]['status'];

                if (($syncStatus & self::STATUS_FINISH) !== self::STATUS_FINISH) {
                    return false;
                }

                if (($status & self::STATUS_LOOP) === self::STATUS_LOOP) {
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
                $this->callStatus[$key]['status'] |= self::STATUS_PROCESS;
                break;
            case self::REQUEST_PRODUCE:
                if (empty($context)) {
                    break;
                }
                $this->callStatus[$key]['status'] |= self::STATUS_PROCESS;
                $contextStatus                     = [];
                if (is_array($context)) {
                    foreach ($context as $fd) {
                        $contextStatus[$fd] = self::STATUS_PROCESS;
                    }
                    $this->callStatus[$key]['context'] = $contextStatus;
                }
                break;
        }
    }

    protected function report()
    {
        //var_dump($this->callStatus[self::REQUEST_COMMIT_OFFSET]);
    }
}
