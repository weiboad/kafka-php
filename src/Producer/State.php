<?php
declare(strict_types=1);

namespace Kafka\Producer;

use Amp\Loop;
use Kafka\ProducerConfig;
use Kafka\SingletonTrait;

class State
{
    use SingletonTrait;

    public const REQUEST_METADATA = 1;
    public const REQUEST_PRODUCE  = 2;

    public const STATUS_INIT    = 0;
    public const STATUS_STOP    = 1;
    public const STATUS_START   = 2;
    public const STATUS_LOOP    = 4;
    public const STATUS_PROCESS = 8;
    public const STATUS_FINISH  = 16;

    /**
     * @var mixed[]
     */
    private $callStatus = [];

    /**
     * @var mixed[][]
     */
    private $requests = [
        self::REQUEST_METADATA => [],
        self::REQUEST_PRODUCE => [],
    ];

    public function init(): void
    {
        $this->callStatus = [
            self::REQUEST_METADATA => ['status' => self::STATUS_LOOP],
            self::REQUEST_PRODUCE  => ['status' => self::STATUS_LOOP],
        ];

        $config = $this->getConfig();

        foreach (\array_keys($this->requests) as $request) {
            if ($request === self::REQUEST_METADATA) {
                $this->requests[$request]['interval'] = $config->getMetadataRefreshIntervalMs();
                break;
            }

            $interval = $config->getIsAsyn() ? $config->getProduceInterval() : 1;

            $this->requests[$request]['interval'] = $interval;
        }
    }

    public function start(): void
    {
        foreach ($this->requests as $request => $option) {
            $interval = $option['interval'] ?? 200;

            Loop::repeat((int) $interval, function (string $watcherId) use ($request, $option): void {
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
    }

    /**
     * @param mixed|null $context
     */
    public function succRun(int $key, $context = null): void
    {
        $config = $this->getConfig();
        $isAsyn = $config->getIsAsyn();

        if (! isset($this->callStatus[$key])) {
            return;
        }

        switch ($key) {
            case self::REQUEST_METADATA:
                $this->callStatus[$key]['status'] = (self::STATUS_LOOP | self::STATUS_FINISH);

                if ($context !== null) { // if kafka broker is change
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

    public function failRun(int $key): void
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

    /**
     * @param callable[] $callbacks
     */
    public function setCallback(array $callbacks): void
    {
        foreach ($callbacks as $request => $callback) {
            $this->requests[$request]['func'] = $callback;
        }
    }

    public function recover(): void
    {
        $this->callStatus = [
            self::REQUEST_METADATA => $this->callStatus[self::REQUEST_METADATA],
            self::REQUEST_PRODUCE => [
                'status'=> self::STATUS_LOOP,
            ],
        ];
    }

    protected function checkRun(int $key): bool
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

        return false;
    }

    /**
     * @param mixed $context
     */
    protected function processing(int $key, $context): void
    {
        if (! isset($this->callStatus[$key])) {
            return;
        }

        // set process start time
        $this->callStatus[$key]['time'] = \microtime(true);
        switch ($key) {
            case self::REQUEST_METADATA:
                $this->callStatus[$key]['status'] |= self::STATUS_PROCESS;
                break;
            case self::REQUEST_PRODUCE:
                if (empty($context)) {
                    break;
                }

                $this->callStatus[$key]['status'] |= self::STATUS_PROCESS;

                $contextStatus = [];

                if (\is_array($context)) {
                    foreach ($context as $fd) {
                        $contextStatus[$fd] = self::STATUS_PROCESS;
                    }

                    $this->callStatus[$key]['context'] = $contextStatus;
                }

                break;
        }
    }

    private function getConfig(): ProducerConfig
    {
        return ProducerConfig::getInstance();
    }
}
