<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace Kafka\Producer;

/**
+------------------------------------------------------------------------------
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class State
{
    use \Kafka\SingletonTrait;
    // {{{ consts

    const REQUEST_METADATA  = 1;
    const REQUEST_PRODUCE  = 2;

    const STATUS_INIT  = 0;
    const STATUS_STOP  = 1;
    const STATUS_START = 2;
    const STATUS_LOOP    = 4;
    const STATUS_PROCESS = 8;
    const STATUS_FINISH  = 16;

    // }}}
    // {{{ members
    
    private $callStatus = array();
    
    private $requests  = array(
        self::REQUEST_METADATA => array(),
        self::REQUEST_PRODUCE => array(),
    );

    // }}}
    // {{{ functions
    // {{{ public function init()

    public function init()
    {
        $this->callStatus = array(
            self::REQUEST_METADATA => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_PRODUCE => array(
                'status'=> self::STATUS_LOOP,
            ),
        );

        // instances clear

        // init requests
        $config = \Kafka\ConsumerConfig::getInstance();
        foreach ($this->requests as $request => $option) {
            switch ($request) {
            case self::REQUEST_METADATA:
                $this->requests[$request]['interval'] = $config->getMetadataRefreshIntervalMs();
                break;
            default:
                $isAsyn = $config->getIsAsyn();
                if ($isAsyn) {
                    $this->requests[$request]['interval'] = $config->getProduceInterval();
                } else {
                    $this->requests[$request]['interval'] = 1;
                }
            }
        }
    }

    // }}}
    // {{{ public function start()

    public function start()
    {
        foreach ($this->requests as $request => $option) {
            $interval = isset($option['interval']) ? $option['interval'] : 200;
            \Amp\repeat(function ($watcherId) use ($request, $option) {
                if ($this->checkRun($request) && $option['func'] != null) {
                    $context = call_user_func($option['func']);
                    $this->processing($request, $context);
                }
                $this->requests[$request]['watcher'] = $watcherId;
            }, $msInterval = $interval);
        }

        // start sync metadata
        if (isset($this->requests[self::REQUEST_METADATA]['func'])
            && $this->callStatus[self::REQUEST_METADATA]['status'] == self::STATUS_LOOP) {
            $context = call_user_func($this->requests[self::REQUEST_METADATA]['func']);
            $this->processing($request, $context);
        }
        \Amp\repeat(function ($watcherId) {
            $this->report();
        }, $msInterval = 1000);
    }

    // }}}
    // {{{ public function succRun()

    public function succRun($key, $context = null)
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $isAsyn = $config->getIsAsyn();
        if (!isset($this->callStatus[$key])) {
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
                if ($context == null) {
                    if (!$isAsyn) {
                        $this->callStatus[$key]['status'] = self::STATUS_FINISH;
                        \Amp\stop();
                    } else {
                        $this->callStatus[$key]['status'] = (self::STATUS_LOOP | self::STATUS_FINISH);
                    }
                    break;
                }
                unset($this->callStatus[$key]['context'][$context]);
                $contextStatus = $this->callStatus[$key]['context'];
                if (empty($contextStatus)) {
                    if (!$isAsyn) {
                        $this->callStatus[$key]['status'] = self::STATUS_FINISH;
                        \Amp\stop();
                    } else {
                        $this->callStatus[$key]['status'] = (self::STATUS_LOOP | self::STATUS_FINISH);
                    }
                }
                break;
        }
    }

    // }}}
    // {{{ public function failRun()

    public function failRun($key, $context = null)
    {
        if (!isset($this->callStatus[$key])) {
            return false;
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

    // }}}
    // {{{ public function setCallback()

    public function setCallback($callbacks)
    {
        foreach ($callbacks as $request => $callback) {
            $this->requests[$request]['func'] = $callback;
        }
    }

    // }}}
    // {{{ public function recover()

    public function recover()
    {
        $this->callStatus = array(
            self::REQUEST_METADATA => $this->callStatus[self::REQUEST_METADATA],
            self::REQUEST_PRODUCE => array(
                'status'=> self::STATUS_LOOP,
            ),
        );
    }

    // }}}
    // {{{ protected function checkRun()

    protected function checkRun($key)
    {
        if (!isset($this->callStatus[$key])) {
            return false;
        }

        $status = $this->callStatus[$key]['status'];
        switch ($key) {
            case self::REQUEST_METADATA:
                if ($status & self::STATUS_PROCESS == self::STATUS_PROCESS) {
                    return false;
                }
                if (($status & self::STATUS_LOOP) == self::STATUS_LOOP) {
                    return true;
                }
                return false;
            case self::REQUEST_PRODUCE:
                if (($status & self::STATUS_PROCESS) == self::STATUS_PROCESS) {
                    return false;
                }
                $syncStatus = $this->callStatus[self::REQUEST_METADATA]['status'];
                if (($syncStatus & self::STATUS_FINISH) != self::STATUS_FINISH) {
                    return false;
                }
                if (($status & self::STATUS_LOOP) == self::STATUS_LOOP) {
                    return true;
                }
                return false;
        }
    }

    // }}}
    // {{{ protected function processing()

    protected function processing($key, $context)
    {
        if (!isset($this->callStatus[$key])) {
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
                $contextStatus = array();
                if (is_array($context)) {
                    foreach ($context as $fd) {
                        $contextStatus[$fd] = self::STATUS_PROCESS;
                    }
                    $this->callStatus[$key]['context'] = $contextStatus;
                }
                break;
        }
    }

    // }}}
    // {{{ protected function report()

    protected function report()
    {
        //var_dump($this->callStatus[self::REQUEST_COMMIT_OFFSET]);
    }

    // }}}
    // }}}
}
