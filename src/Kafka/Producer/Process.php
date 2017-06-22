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

    protected $success = null;

    protected $error = null;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    public function __construct(\Closure $producer = null)
    {
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
        $broker->setProcess(function ($data, $fd) {
            $this->processRequest($data, $fd);
        });

        // init state
        $this->state = \Kafka\Producer\State::getInstance();
        if ($this->logger) {
            $this->state->setLogger($this->logger);
        }
        $this->state->setCallback(array(
            \Kafka\Producer\State::REQUEST_METADATA => function () {
                return $this->syncMeta();
            },
            \Kafka\Producer\State::REQUEST_PRODUCE => function () {
                return $this->produce();
            },
        ));
        $this->state->init();

        $topics = $broker->getTopics();
        if (!empty($topics)) {
            $this->state->succRun(\Kafka\Producer\State::REQUEST_METADATA);
        }
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
        $config = \Kafka\ProducerConfig::getInstance();
        $isAsyn = $config->getIsAsyn();
        if (!$isAsyn) {
            \Amp\repeat(function ($watcherId) {
                if ($this->error) {
                    call_user_func($this->error, 1000);
                }
                \Amp\cancel($watcherId);
                \Amp\stop();
            }, $msInterval = $config->getRequestTimeout());
        };
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
    // {{{ public function setSuccess()

    /**
     * set success callback
     *
     * @access public
     * @return void
     */
    public function setSuccess($success)
    {
        $this->success = $success;
    }

    // }}}
    // {{{ public function setError()

    /**
     * set error callback
     *
     * @access public
     * @return void
     */
    public function setError($error)
    {
        $this->error = $error;
    }

    // }}}
    // {{{ public function syncMeta()

    public function syncMeta()
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
        switch ($correlationId) {
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
            $this->error('Error request, correlationId:' . $correlationId);
        }
    }

    // }}}
    // {{{ protected function produce()

    protected function produce()
    {
        $context = array();
        $broker = \Kafka\Broker::getInstance();
        $requiredAck = \Kafka\ProducerConfig::getInstance()->getRequiredAck();
        $timeout = \Kafka\ProducerConfig::getInstance()->getTimeout();

        // get send message
        // data struct
        //  topic:
        //  partId:
        //  key:
        //  value:
        $data = call_user_func($this->producer);
        if (empty($data)) {
            return $context;
        }

        $sendData = $this->convertMessage($data);
        foreach ($sendData as $brokerId => $topicList) {
            $connect = $broker->getDataConnect($brokerId);
            if (!$connect) {
                return;
            }

            $requiredAck = \Kafka\ProducerConfig::getInstance()->getRequiredAck();
            $params = array(
                'required_ack' => $requiredAck,
                'timeout' => \Kafka\ProducerConfig::getInstance()->getTimeout(),
                'data' => $topicList,
            );
            $this->debug("Send message start, params:" . json_encode($params));
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::PRODUCE_REQUEST, $params);
            if ($requiredAck == 0) { // If it is 0 the server will not send any response
                $this->state->succRun(\Kafka\Producer\State::REQUEST_PRODUCE);
            } else {
                $connect->write($requestData);
                $context[] = (int)$connect->getSocket();
            }
        }

        return $context;
    }

    // }}}
    // {{{ protected function succProduce()

    protected function succProduce($result, $fd)
    {
        $msg = sprintf('Send message sucess, result: %s', json_encode($result));
        $this->debug($msg);
        if ($this->success) {
            call_user_func($this->success, $result);
        }
        $this->state->succRun(\Kafka\Producer\State::REQUEST_PRODUCE, $fd);
    }

    // }}}
    // {{{ protected function stateConvert()

    protected function stateConvert($errorCode, $context = null)
    {
        $retry = false;
        $this->error(\Kafka\Protocol::getError($errorCode));
        if ($this->error) {
            call_user_func($this->error, $errorCode);
        }
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
    // {{{ protected function convertMessage()

    protected function convertMessage($data)
    {
        $sendData = array();
        $broker = \Kafka\Broker::getInstance();
        $topicInfos = $broker->getTopics();
        foreach ($data as $value) {
            if (!isset($value['topic']) || !trim($value['topic'])) {
                continue;
            }

            if (!isset($topicInfos[$value['topic']])) {
                continue;
            }

            if (!isset($value['value']) || !trim($value['value'])) {
                continue;
            }

            if (!isset($value['key'])) {
                $value['key'] = '';
            }

            $topicMeta = $topicInfos[$value['topic']];
            $partNums = array_keys($topicMeta);
            shuffle($partNums);
            $partId = 0;
            if (!isset($value['partId']) || !isset($topicMeta[$value['partId']])) {
                $partId = $partNums[0];
            } else {
                $partId = $value['partId'];
            }

            $brokerId = $topicMeta[$partId];
            $topicData = array();
            if (isset($sendData[$brokerId][$value['topic']])) {
                $topicData = $sendData[$brokerId][$value['topic']];
            }

            $partition = array();
            if (isset($topicData['partitions'][$partId])) {
                $partition = $topicData['partitions'][$partId];
            }

            $partition['partition_id'] = $partId;
            if (trim($value['key']) != '') {
                $partition['messages'][] = array('value' => $value['value'], 'key' => $value['key']);
            } else {
                $partition['messages'][] = $value['value'];
            }
            
            $topicData['partitions'][$partId] = $partition;
            $topicData['topic_name'] = $value['topic'];
            $sendData[$brokerId][$value['topic']] = $topicData;
        }

        return $sendData;
    }

    // }}}
    // }}}
}
