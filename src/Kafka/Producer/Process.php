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

class Process
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    // {{{ consts
    // }}}
    // {{{ members

    protected $producer = null;

    protected $isRunning = true;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    public function __construct(\Closure $producer = null) {
        $this->producer = $producer; 
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
        // init protocol
        $config = \Kafka\ProducerConfig::getInstance();
        \Kafka\Protocol::init($config->getBrokerVersion(), $this->logger);

        // init process request
        $broker = \Kafka\Broker::getInstance();
        $broker->setProcess(function($data, $fd) {
            $this->processRequest($data, $fd);
        });

        // init state
        $this->state = \Kafka\Producer\State::getInstance();
        if ($this->logger) {
            $this->state->setLogger($this->logger);
        }
        $this->state->setCallback(array(
            \Kafka\Producer\State::REQUEST_METADATA => function() {
                return $this->syncMeta();
            },
            \Kafka\Producer\State::REQUEST_PRODUCE => function() {
                return $this->produce(); 
            },
        ));
        $this->state->init();
    }

    // }}}
    // {{{ public function start()

    /**
     * start consumer 
     *
     * @access public
     * @return void
     */
    public function start()
    {
        $this->init();
        $this->state->start();
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
    protected function processRequest($data, $fd)
    {
        $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
        switch($correlationId) {
        case \Kafka\Protocol::METADATA_REQUEST:
            $result = \Kafka\Protocol::decode(\Kafka\Protocol::METADATA_REQUEST, substr($data, 4));
            if (!isset($result['brokers']) || !isset($result['topics'])) {
                $this->error('Get metadata is fail, brokers or topics is null.');
                $this->state->failRun(\Kafka\Producer\State::REQUEST_METADATA);
            } else {
                $broker = \Kafka\Broker::getInstance();
                $isChange = $broker->setData($result['topics'], $result['brokers']);
                $this->state->succRun(\Kafka\Producer\State::REQUEST_METADATA, $isChange);
            }
            break;
        case \Kafka\Protocol::PRODUCE_REQUEST:
            $result = \Kafka\Protocol::decode(\Kafka\Protocol::PRODUCE_REQUEST, substr($data, 4));
            $this->succProduce($result, $fd);
            break;
        default:
            var_dump($correlationId);
        }
    }

    // }}}
    // {{{ protected function syncMeta()

    protected function syncMeta()
    {
        $this->debug('Start sync metadata request');
        $brokerList = explode(',', \Kafka\ProducerConfig::getInstance()->getMetadataBrokerList());
        $brokerHost = array();
        foreach ($brokerList as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = $val;
            }
        }
        if (count($brokerHost) == 0) {
            throw new \Kafka\Exception('Not set config `metadataBrokerList`');
        }
        shuffle($brokerHost);
        $broker = \Kafka\Broker::getInstance();
        foreach ($brokerHost as $host) {
            $socket = $broker->getMetaConnect($host);
            if ($socket) {
                $params = array();
                $this->debug('Start sync metadata request params:' . json_encode($params));
                $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::METADATA_REQUEST, $params);
                $socket->write($requestData);
                return;
            }
        }
        throw new \Kafka\Exception('Not has broker can connection `metadataBrokerList`');
    }

    // }}}
    // {{{ protected function produce()

    protected function produce()
    {
        $context = array();
        $requiredAck = \Kafka\ProducerConfig::getInstance()->getRequiredAck();
        $timeout = \Kafka\ProducerConfig::getInstance()->getTimeout();

        // get send message
        // data struct
        //  topic:
        //  partId:
        //  key:
        //  value:
        $data = call_user_func($this->producer);
        var_dump($data);

        return $context;
    }

    // }}}
    // {{{ protected function succProduce()

    protected function succProduce($result, $fd)
    {
        $msg = sprintf('Send message sucess, result: %s', json_encode($result));
        $this->debug($msg);
        $this->state->succRun(\Kafka\Consumer\State::REQUEST_PRODUCE, $fd);
    }

    // }}}
    // {{{ protected function stateConvert()

    protected function stateConvert($errorCode, $context = null)
    {
        $retry = false;
        $this->error(\Kafka\Protocol::getError($errorCode));
        $recoverCodes = array(
            \Kafka\Protocol::UNKNOWN_TOPIC_OR_PARTITION,
            \Kafka\Protocol::INVALID_REQUIRED_ACKS,
            \Kafka\Protocol::RECORD_LIST_TOO_LARGE,
            \Kafka\Protocol::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            \Kafka\Protocol::NOT_ENOUGH_REPLICAS,
            \Kafka\Protocol::NOT_LEADER_FOR_PARTITION,
            \Kafka\Protocol::BROKER_NOT_AVAILABLE,
            \Kafka\Protocol::GROUP_LOAD_IN_PROGRESS,
            \Kafka\Protocol::GROUP_COORDINATOR_NOT_AVAILABLE,
            \Kafka\Protocol::NOT_COORDINATOR_FOR_GROUP,
            \Kafka\Protocol::INVALID_TOPIC,
            \Kafka\Protocol::INCONSISTENT_GROUP_PROTOCOL,
            \Kafka\Protocol::INVALID_GROUP_ID,
        );
        if (in_array($errorCode, $recoverCodes)) {
            $this->state->recover();
            return false;
        }
        return true;
    }

    // }}}
    // }}}
}
