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

namespace Kafka;

/**
+------------------------------------------------------------------------------
* Kafka Broker info manager
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Broker
{
    use SingletonTrait;
    // {{{ consts
    // }}}
    // {{{ members

    private $groupBrokerId = null;

    private $topics = array();

    private $brokers = array();

    private $metaSockets = array();

    private $dataSockets = array();

    private $process;

    private $socket;

    // }}}
    // {{{ functions
    // {{{ public function setProcess()

    public function setProcess(\Closure $process)
    {
        $this->process = $process;
    }

    // }}}
    // {{{ public function setGroupBrokerId()

    public function setGroupBrokerId($brokerId)
    {
        $this->groupBrokerId = $brokerId;
    }

    // }}}
    // {{{ public function getGroupBrokerId()

    public function getGroupBrokerId()
    {
        return $this->groupBrokerId;
    }

    // }}}
    // {{{ public function setData()

    public function setData($topics, $brokersResult)
    {
        $brokers = array();
        foreach ($brokersResult as $value) {
            $key = $value['nodeId'];
            $hostname = $value['host'] . ':' . $value['port'];
            $brokers[$key] = $hostname;
        }

        $change = false;
        if (serialize($this->brokers) != serialize($brokers)) {
            $this->brokers = $brokers;
            $change = true;
        }

        $newTopics = array();
        foreach ($topics as $topic) {
            if ($topic['errorCode'] != \Kafka\Protocol::NO_ERROR) {
                $this->error('Parse metadata for topic is error, error:' . \Kafka\Protocol::getError($topic['errorCode']));
                continue;
            }
            $item = array();
            foreach ($topic['partitions']  as $part) {
                $item[$part['partitionId']] = $part['leader'];
            }
            $newTopics[$topic['topicName']] = $item;
        }

        if (serialize($this->topics) != serialize($newTopics)) {
            $this->topics = $newTopics;
            $change = true;
        }

        return $change;
    }

    // }}}
    // {{{ public function getTopics()

    public function getTopics()
    {
        return $this->topics;
    }

    // }}}
    // {{{ public function getBrokers()

    public function getBrokers()
    {
        return $this->brokers;
    }

    // }}}
    // {{{ public function getMetaConnect()

    public function getMetaConnect($key, $modeSync = false)
    {
        return $this->getConnect($key, 'metaSockets', $modeSync);
    }

    // }}}
    // {{{ public function getRandConnect()

    public function getRandConnect($modeSync = false)
    {
        $nodeIds = array_keys($this->brokers);
        shuffle($nodeIds);
        if (!isset($nodeIds[0])) {
            return false;
        }
        return $this->getMetaConnect($nodeIds[0], $modeSync);
    }

    // }}}
    // {{{ public function getDataConnect()
    
    public function getDataConnect($key, $modeSync = false)
    {
        return $this->getConnect($key, 'dataSockets', $modeSync);
    }

    // }}}
    // {{{ public function getConnect()

    public function getConnect($key, $type, $modeSync = false)
    {
        if (isset($this->{$type}[$key])) {
            return $this->{$type}[$key];
        }

        if (isset($this->brokers[$key])) {
            $hostname = $this->brokers[$key];
            if (isset($this->{$type}[$hostname])) {
                return $this->{$type}[$hostname];
            }
        }

        $host = null;
        $port = null;
        if (isset($this->brokers[$key])) {
            $hostname = $this->brokers[$key];
            list($host, $port) = explode(':', $hostname);
        }

        if (strpos($key, ':') !== false) {
            list($host, $port) = explode(':', $key);
        }

        if ($host && $port) {
            try {
                $socket = $this->getSocket($host, $port, $modeSync);
                if (!$modeSync) {
                    $socket->SetonReadable($this->process);
                }
                $socket->connect();
                $this->{$type}[$key] = $socket;
                return $socket;
            } catch (\Exception $e) {
                $this->error($e->getMessage());
                return false;
            }
        } else {
            return false;
        }
    }

    // }}}
    // {{{ public function clear()

    public function clear()
    {
        foreach ($this->metaSockets as $key => $socket) {
            $socket->close();
        }
        foreach ($this->dataSockets as $key => $socket) {
            $socket->close();
        }
        $this->brokers = [];
    }

    // }}}
    // {{{ public function getSocket()

    public function getSocket($host, $port, $modeSync)
    {
        if ($this->socket != null) {
            return $this->socket;
        }

        if ($modeSync) {
            $socket = new \Kafka\SocketSync($host, $port);
        } else {
            $socket = new \Kafka\Socket($host, $port);
        }
        return $socket;
    }

    // }}}
    // {{{ public function setSocket()

    // use unit test
    public function setSocket($socket)
    {
        $this->socket = $socket;
    }

    // }}}
    // }}}
}
