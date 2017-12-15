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

use \Kafka\Sasl\Plain;
use \Kafka\Sasl\Gssapi;
use \Kafka\Sasl\Scram;

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

    private $topics = [];

    private $brokers = [];

    private $metaSockets = [];

    private $dataSockets = [];

    private $process;

    private $config;

    // }}}
    // {{{ functions
    // {{{ public function setProcess()

    public function setProcess(callable $process)
    {
        $this->process = $process;
    }

    // }}}
    // {{{ public function setConfig()

    public function setConfig(Config $config)
    {
        $this->config = $config;
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
        $brokers = [];
        foreach ($brokersResult as $value) {
            $key           = $value['nodeId'];
            $hostname      = $value['host'] . ':' . $value['port'];
            $brokers[$key] = $hostname;
        }

        $change = false;
        if (serialize($this->brokers) != serialize($brokers)) {
            $this->brokers = $brokers;
            $change        = true;
        }

        $newTopics = [];
        foreach ($topics as $topic) {
            if ($topic['errorCode'] != \Kafka\Protocol::NO_ERROR) {
                $this->error('Parse metadata for topic is error, error:' . \Kafka\Protocol::getError($topic['errorCode']));
                continue;
            }
            $item = [];
            foreach ($topic['partitions'] as $part) {
                $item[$part['partitionId']] = $part['leader'];
            }
            $newTopics[$topic['topicName']] = $item;
        }

        if (serialize($this->topics) != serialize($newTopics)) {
            $this->topics = $newTopics;
            $change       = true;
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
        if (! isset($nodeIds[0])) {
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
            $hostname          = $this->brokers[$key];
            list($host, $port) = explode(':', $hostname);
        }

        if (strpos($key, ':') !== false) {
            list($host, $port) = explode(':', $key);
        }

        if (! $host || ! $port || (! $modeSync && ! $this->process)) {
            return false;
        }

        try {
            $socket = $this->getSocket($host, $port, $modeSync);
            if (! $modeSync) {
                $socket->setOnReadable($this->process);
            }
            $socket->connect();
            $this->{$type}[$key] = $socket;

            return $socket;
        } catch (\Exception $e) {
            $this->error($e->getMessage());
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
        $saslProvider = $this->judgeConnectionConfig();
        if ($modeSync) {
            $socket = new \Kafka\SocketSync($host, $port, $this->config, $saslProvider);
        } else {
            $socket = new \Kafka\Socket($host, $port, $this->config, $saslProvider);
        }
        return $socket;
    }

    // }}}
    // {{{ private function judgeConnectionConfig()

    private function judgeConnectionConfig() : ?SaslMechanism
    {
        if ($this->config == null) {
            return null;
        }

        $plainConnections = [
            Config::SECURITY_PROTOCOL_PLAINTEXT,
            Config::SECURITY_PROTOCOL_SASL_PLAINTEXT
        ];
        $saslConnections  = [
            Config::SECURITY_PROTOCOL_SASL_SSL,
            Config::SECURITY_PROTOCOL_SASL_PLAINTEXT
        ];
        
        $securityProtocol = $this->config->getSecurityProtocol();
        if (in_array($securityProtocol, $plainConnections, true)) {
            $this->config->setSslEnable(false);
        } else {
            $this->config->setSslEnable(true);
        }

        if (in_array($securityProtocol, $saslConnections, true)) {
            return $this->getSaslMechanismProvider();
        }

        return null;
    }

    // }}}
    // {{{ private function getSaslMechanismProvider()

    private function getSaslMechanismProvider() : SaslMechanism
    {
        $mechanism = $this->config->getSaslMechanism();
        $provider  = null;
        $username  = $this->config->getSaslUsername();
        $password  = $this->config->getSaslPassword();
        switch ($mechanism) {
            case Config::SASL_MECHANISMS_PLAIN:
                $provider = new Plain($username, $password);
                break;
            case Config::SASL_MECHANISMS_GSSAPI:
                $provider = Gssapi::fromKeytab($this->config->getSaslKeytab(), $this->config->getSaslPrincipal());
                break;
            case Config::SASL_MECHANISMS_SCRAM_SHA_256:
                $provider = new Scram($username, $password, Scram::SCRAM_SHA_256);
                break;
            case Config::SASL_MECHANISMS_SCRAM_SHA_512:
                $provider = new Scram($username, $password, Scram::SCRAM_SHA_512);
                break;
        }
        return $provider;
    }

    // }}}
    // }}}
}
