<?php
namespace Kafka\Producer;

use Kafka\Broker;
use Kafka\LoggerTrait;
use Kafka\lib\ProducerConfig;
use Psr\Log\LoggerAwareInterface;

class SyncProcess implements LoggerAwareInterface
{
    use LoggerTrait;


    /**
     * @var Broker
     */
    private $broker;


    /**
     * @var ProducerConfig
     */
    private $config;

    public function __construct(ProducerConfig $config = null)
    {
        $this->setConfig($config);

        $config = \Kafka\lib\ProducerConfig::getInstance();
        \Kafka\Protocol::init($config->getBrokerVersion(), $this->logger);

        $broker = new Broker();
        $broker->setConfig($config);

        $this->setBroker($broker);

        $this->syncMeta();
    }

    /**
     * @param $data
     *
     * @return array|bool
     */
    public function send($data)
    {
        $broker      = $this->getBroker();
        $config = $this->getConfig();
        $requiredAck = $config->getRequiredAck();
        $timeout     = $config->getTimeout();

        if (empty($data)) {
            return false;
        }

        $sendData = $this->convertMessage($data);
        $result   = [];
        foreach ($sendData as $brokerId => $topicList) {
            $connect = $broker->getDataConnect($brokerId, true);
            if (! $connect) {
                return false;
            }

            $params      = [
                'required_ack' => $requiredAck,
                'timeout' => $timeout,
                'data' => $topicList,
            ];
            $this->debug("Send message start, params:" . json_encode($params));
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::PRODUCE_REQUEST, $params);
            $connect->write($requestData);
            if ($requiredAck != 0) { // If it is 0 the server will not send any response
                $dataLen       = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $connect->read(4));
                $data          = $connect->read($dataLen);
//                $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
                $ret           = \Kafka\Protocol::decode(\Kafka\Protocol::PRODUCE_REQUEST, substr($data, 4));
                $result[]      = $ret;
            }
        }
        return $result;
    }

    public function syncMeta()
    {
        $this->debug('Start sync metadata request');

        $config = $this->getConfig();
        $broker = $this->getBroker();

        $brokerList = $config->getMetadataBrokerList();
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

        foreach ($brokerHost as $host) {
            $socket = $broker->getMetaConnect($host, true);
            if ($socket) {
                $params = [];
                $this->debug('Start sync metadata request params:' . json_encode($params));
                $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::METADATA_REQUEST, $params);
                $socket->write($requestData);
                $dataLen       = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $socket->read(4));
                $data          = $socket->read($dataLen);
                $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
                $result        = \Kafka\Protocol::decode(\Kafka\Protocol::METADATA_REQUEST, substr($data, 4));
                if (! isset($result['brokers']) || ! isset($result['topics'])) {
                    throw new \Kafka\Exception('Get metadata is fail, brokers or topics is null.');
                } else {
                    $broker = \Kafka\Broker::getInstance(__CLASS__);
                    $broker->setData($result['topics'], $result['brokers']);
                }
                //TODO why return ？？
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

    protected function convertMessage($data)
    {
        $sendData   = [];
        $broker     = $this->getBroker();
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

            if (! isset($value['partId']) || ! isset($topicMeta[$value['partId']])) {
                $partNums  = array_keys($topicMeta);
                shuffle($partNums);
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

    /**
     * @return Broker
     */
    public function getBroker(): Broker
    {
        return $this->broker;
    }

    /**
     * @param Broker $broker
     */
    public function setBroker(Broker $broker)
    {
        $this->broker = $broker;
    }

    /**
     * @return ProducerConfig
     */
    public function getConfig(): ProducerConfig
    {
        return $this->config;
    }

    /**
     * @param ProducerConfig $config
     */
    public function setConfig(ProducerConfig $config)
    {
        $this->config = $config;
    }
}
