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

class Process
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    // {{{ consts
    // }}}
    // {{{ members

    protected $consumer = null;

    protected $isRunning = true;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    public function __construct(\Closure $consumer = null) {
        $this->consumer = $consumer; 
    }

    // }}}
    // {{{ public function init()

    /**
     * start consumer 
     *
     * @access public
     * @return void
     */
    public function init()
    {
        $this->state = \Kafka\Consumer\State::getInstance();
        if ($this->logger) {
            $this->state->setLogger($this->logger);
        }
        // init process request
        $connections = \Kafka\Consumer\Connection::getInstance();
        $connections->setProcess(function($data) {
            $this->processRequest($data);
        });

        $this->state->waitSyncMeta();

        // repeat get update meta info
        \Amp\repeat(function ($watcherId) {
            $this->state->waitSyncMeta();
            if (!$this->isRunning) {
                \Amp\cancel($watcherId);
            }
        }, $msInterval = \Kafka\ConsumerConfig::getInstance()->getMetadataRefreshIntervalMs());
    }

    // }}}
    // {{{ public function stop()

    /**
     * stop consumer 
     *
     * @access public
     * @return void
     */
    public function stop()
    {
        $this->isRunning = false;
    }

    // }}}
    // {{{ protected function processRequest()

    /**
     * process Request 
     *
     * @access public
     * @return void
     */
    protected function processRequest($data)
    {
        $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
        $connections = \Kafka\Consumer\Connection::getInstance();
        switch($correlationId) {
        case \Kafka\Protocol\Protocol::METADATA_REQUEST:
            $meta = new \Kafka\Protocol\Metadata(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
            $result = $meta->decode(substr($data, 4));
            if (!isset($result['brokers']) || !isset($result['topics'])) {
                $this->error('Get metadata is fail, brokers or topics is null.');
                $this->state->failSyncMeta();
            } else {
                $this->state->succSyncMeta($result['brokers'], $result['topics']);
            }
            break;
        case \Kafka\Protocol\Protocol::GROUP_COORDINATOR_REQUEST:
            $group = new \Kafka\Protocol\GroupCoordinator(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
            $result = $group->decode(substr($data, 4));
            if (isset($result['errorCode']) && $result['errorCode'] == 0) {
                if (isset($result['coordinatorId'])) {
                    $this->state->succGetGroupBrokerId($result['coordinatorId']);
                } else { // sync brokers meta
                    $this->state->failGetGroupBrokerId(-1);
                }
            } else {
                $this->state->failGetGroupBrokerId($result['errorCode']);
            }
            break;
        case \Kafka\Protocol\Protocol::JOIN_GROUP_REQUEST:
            $group = new \Kafka\Protocol\JoinGroup(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
            $result = $group->decode(substr($data, 4));
            if (isset($result['errorCode']) && $result['errorCode'] == 0) {
                $this->state->succJoinGroup($result);
            } else {
                $this->state->failJoinGroup($result['errorCode']);
            }
            break;
        case \Kafka\Protocol\Protocol::SYNC_GROUP_REQUEST:
            $group = new \Kafka\Protocol\SyncGroup(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
            $result = $group->decode(substr($data, 4));
            if (isset($result['errorCode']) && $result['errorCode'] == 0) {
                $this->state->succSyncGroup($result);
            } else {
                $this->state->failSyncGroup($result['errorCode']);
            }
            break;
        case \Kafka\Protocol\Protocol::HEART_BEAT_REQUEST:
            $heart = new \Kafka\Protocol\Heartbeat(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
            $result = $heart->decode(substr($data, 4));
            if (isset($result['errorCode']) && $result['errorCode'] == 0) {
                $this->state->succHeartbeat($result);
            } else {
                $this->state->failHeartbeat($result['errorCode']);
            }
            break;
        }
    }

    // }}}
    // }}}
}
