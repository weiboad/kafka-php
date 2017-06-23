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

namespace Kafka\Consumer;

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
    const REQUEST_GETGROUP  = 2;
    const REQUEST_JOINGROUP = 3;
    const REQUEST_SYNCGROUP = 4;
    const REQUEST_HEARTGROUP = 5;
    const REQUEST_OFFSET = 6;
    const REQUEST_FETCH = 7;
    const REQUEST_FETCH_OFFSET = 8;
    const REQUEST_COMMIT_OFFSET = 9;

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
        self::REQUEST_GETGROUP => array(),
        self::REQUEST_JOINGROUP => array(),
        self::REQUEST_SYNCGROUP => array(),
        self::REQUEST_HEARTGROUP => array(),
        self::REQUEST_OFFSET => array(
            'interval' => 2000,
        ),
        self::REQUEST_FETCH => array(
            'interval' => 100,
        ),
        self::REQUEST_FETCH_OFFSET => array(
            'interval' => 2000,
        ),
        self::REQUEST_COMMIT_OFFSET => array(
            'norepeat' => true,
        ),
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
            self::REQUEST_GETGROUP => array(
                'status'=> self::STATUS_START,
            ),
            self::REQUEST_JOINGROUP => array(
                'status'=> self::STATUS_START,
            ),
            self::REQUEST_SYNCGROUP => array(
                'status'=> self::STATUS_START,
            ),
            self::REQUEST_HEARTGROUP => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_OFFSET => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_FETCH => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_FETCH_OFFSET => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_COMMIT_OFFSET => array(
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
                $this->requests[$request]['interval'] = 1000;
            }
        }
    }

    // }}}
    // {{{ public function start()

    public function start()
    {
        foreach ($this->requests as $request => $option) {
            if (isset($option['norepeat']) && $option['norepeat']) {
                continue;
            }
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
        if (isset($this->requests[self::REQUEST_METADATA]['func'])) {
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
                if (!isset($this->callStatus[$key]['context'])) {
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
                if (!isset($this->callStatus[$key]['context'])) {
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
            case self::REQUEST_GETGROUP:
            case self::REQUEST_JOINGROUP:
            case self::REQUEST_SYNCGROUP:
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
    // {{{ public function rejoin()

    public function rejoin()
    {
        $joinGroupStatus = $this->callStatus[self::REQUEST_JOINGROUP]['status'];
        if (($joinGroupStatus & self::STATUS_PROCESS) == self::STATUS_PROCESS) {
            return;
        }

        $this->callStatus = array(
            self::REQUEST_METADATA => $this->callStatus[self::REQUEST_METADATA],
            self::REQUEST_GETGROUP => $this->callStatus[self::REQUEST_GETGROUP],
            self::REQUEST_JOINGROUP => array(
                'status'=> self::STATUS_START,
            ),
            self::REQUEST_SYNCGROUP => array(
                'status'=> self::STATUS_START,
            ),
            self::REQUEST_HEARTGROUP => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_OFFSET => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_FETCH => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_FETCH_OFFSET => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_COMMIT_OFFSET => array(
                'status'=> self::STATUS_LOOP,
            ),
        );
    }

    // }}}
    // {{{ public function recover()

    public function recover()
    {
        $this->callStatus = array(
            self::REQUEST_METADATA => $this->callStatus[self::REQUEST_METADATA],
            self::REQUEST_GETGROUP => array(
                'status'=> self::STATUS_START,
            ),
            self::REQUEST_JOINGROUP => array(
                'status'=> self::STATUS_START,
            ),
            self::REQUEST_SYNCGROUP => array(
                'status'=> self::STATUS_START,
            ),
            self::REQUEST_HEARTGROUP => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_OFFSET => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_FETCH => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_FETCH_OFFSET => array(
                'status'=> self::STATUS_LOOP,
            ),
            self::REQUEST_COMMIT_OFFSET => array(
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
                $contextStatus = array();
                foreach ($context as $fd) {
                    $contextStatus[$fd] = self::STATUS_PROCESS;
                }
                $this->callStatus[$key]['context'] = $contextStatus;
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
