<?php
namespace Kafka\Producer;

use Amp\Loop;

class Process
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    protected $producer = null;

    protected $isRunning = true;

    protected $success = null;

    protected $error = null;

    private $state;

    public function __construct(callable $producer = null)
    {
        $this->producer = $producer;
    }

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
        $broker->setConfig($config);
        $broker->setProcess(function ($data, $fd) {
            $this->processRequest($data, $fd);
        });

        // init state
        $this->state = \Kafka\Producer\State::getInstance();
        if ($this->logger) {
            $this->state->setLogger($this->logger);
        }
        $this->state->setCallback([
            \Kafka\Producer\State::REQUEST_METADATA => function () {
                return $this->syncMeta();
            },
            \Kafka\Producer\State::REQUEST_PRODUCE => function () {
                return $this->produce();
            },
        ]);
        $this->state->init();

        $topics = $broker->getTopics();
        if (! empty($topics)) {
            $this->state->succRun(\Kafka\Producer\State::REQUEST_METADATA);
        }
    }

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
        if (! $isAsyn) {
            Loop::repeat($config->getRequestTimeout(), function ($watcherId) {
                if ($this->error) {
                    ($this->error)(1000);
                }

                Loop::cancel($watcherId);
                Loop::stop();
            });
        };
    }

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

    public function syncMeta()
    {
        $this->debug('Start sync metadata request');

        $brokerList = \Kafka\ProducerConfig::getInstance()->getMetadataBrokerList();
        $brokerHost = [];

        foreach (explode(',', $brokerList) as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = $val;
            }
        }

        if (count($brokerHost) == 0) {
            throw new \Kafka\Exception('No valid broker configured');
        }

        shuffle($brokerHost);
        $broker = \Kafka\Broker::getInstance();
        foreach ($brokerHost as $host) {
            $socket = $broker->getMetaConnect($host);
            if ($socket) {
                $params = [];
                $this->debug('Start sync metadata request params:' . json_encode($params));
                $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::METADATA_REQUEST, $params);
                $socket->write($requestData);
                return;
            }
        }

        throw new \Kafka\Exception(
            sprintf(
                'It was not possible to establish a connection for metadata with the brokers "%s"',
                $brokerList
            )
        );
    }

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
                if (! isset($result['brokers']) || ! isset($result['topics'])) {
                    $this->error('Get metadata is fail, brokers or topics is null.');
                    $this->state->failRun(\Kafka\Producer\State::REQUEST_METADATA);
                } else {
                    $broker   = \Kafka\Broker::getInstance();
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

    protected function produce(): array
    {
        $context     = [];
        $broker      = \Kafka\Broker::getInstance();
        $requiredAck = \Kafka\ProducerConfig::getInstance()->getRequiredAck();
        $timeout     = \Kafka\ProducerConfig::getInstance()->getTimeout();

        // get send message
        // data struct
        //  topic:
        //  partId:
        //  key:
        //  value:
        $data = ($this->producer)();

        if (empty($data)) {
            return $context;
        }

        $sendData = $this->convertMessage($data);

        foreach ($sendData as $brokerId => $topicList) {
            $connect = $broker->getDataConnect($brokerId);

            if (! $connect) {
                return $context;
            }

            $params = [
                'required_ack' => $requiredAck,
                'timeout'      => $timeout,
                'data'         => $topicList,
            ];

            $this->debug("Send message start, params:" . json_encode($params));
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::PRODUCE_REQUEST, $params);

            if ($requiredAck == 0) { // If it is 0 the server will not send any response
                $this->state->succRun(\Kafka\Producer\State::REQUEST_PRODUCE);
            } else {
                $connect->write($requestData);
                $context[] = (int) $connect->getSocket();
            }
        }

        return $context;
    }

    protected function succProduce($result, $fd)
    {
        $msg = sprintf('Send message sucess, result: %s', json_encode($result));
        $this->debug($msg);

        if ($this->success) {
            ($this->success)($result);
        }
        $this->state->succRun(\Kafka\Producer\State::REQUEST_PRODUCE, $fd);
    }

    protected function stateConvert($errorCode, $context = null)
    {
        $retry = false;
        $this->error(\Kafka\Protocol::getError($errorCode));
        if ($this->error) {
            ($this->error)($errorCode);
        }

        $recoverCodes = [
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
        ];
        if (in_array($errorCode, $recoverCodes)) {
            $this->state->recover();
            return false;
        }

        return true;
    }

    protected function convertMessage($data)
    {
        $sendData   = [];
        $broker     = \Kafka\Broker::getInstance();
        $topicInfos = $broker->getTopics();
        foreach ($data as $value) {
            if (! isset($value['topic']) || ! trim($value['topic'])) {
                continue;
            }

            if (! isset($topicInfos[$value['topic']])) {
                continue;
            }

            if (! isset($value['value']) || ! trim($value['value'])) {
                continue;
            }

            if (! isset($value['key'])) {
                $value['key'] = '';
            }

            $topicMeta = $topicInfos[$value['topic']];
            $partNums  = array_keys($topicMeta);
            shuffle($partNums);
            $partId = 0;
            if (! isset($value['partId']) || ! isset($topicMeta[$value['partId']])) {
                $partId = $partNums[0];
            } else {
                $partId = $value['partId'];
            }

            $brokerId  = $topicMeta[$partId];
            $topicData = [];
            if (isset($sendData[$brokerId][$value['topic']])) {
                $topicData = $sendData[$brokerId][$value['topic']];
            }

            $partition = [];
            if (isset($topicData['partitions'][$partId])) {
                $partition = $topicData['partitions'][$partId];
            }

            $partition['partition_id'] = $partId;
            if (trim($value['key']) != '') {
                $partition['messages'][] = ['value' => $value['value'], 'key' => $value['key']];
            } else {
                $partition['messages'][] = $value['value'];
            }

            $topicData['partitions'][$partId]     = $partition;
            $topicData['topic_name']              = $value['topic'];
            $sendData[$brokerId][$value['topic']] = $topicData;
        }

        return $sendData;
    }
}
