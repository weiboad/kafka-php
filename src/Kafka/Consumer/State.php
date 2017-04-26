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
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    // {{{ consts

    const REQUEST_GETGROUP  = 1;
    const REQUEST_JOINGROUP = 2;
    const REQUEST_SYNCGROUP = 3;
    const REQUEST_HEARTGROUP = 4;
    const REQUEST_OFFSET = 5;
    const REQUEST_FETCH = 6;
    const REQUEST_FETCH_OFFSET = 7;
    const REQUEST_COMMIT_OFFSET = 8;

    const STATUS_STOP = 0;
    const STATUS_START = 1;
    const STATUS_PROCESS = 2;

    const OFFSET_OUT_OF_RANGE = 1;
    const UNKNOWN_TOPIC_OR_PARTITION = 3;
    const NOT_LEADER_FOR_PARTITION = 6;
    const GROUP_LOAD_IN_PROGRESS = 14;
    const GROUP_COORDINATOR_NOT_AVAILABLE = 15;
    const NOT_COORDINATOR_FOR_GROUP = 16;
    const ILLEGAL_GENERATION = 22;
    const INCONSISTENT_GROUP_PROTOCOL = 23;
    const INVALID_GROUP_ID_CODE = 24;
    const UNKNOWN_MEMBER_ID = 25; 
    const REBALANCE_IN_PROGRESS = 27;
    const INVALID_SESSION_TIMEOUT = 26;
    const INVALID_COMMIT_OFFSET_SIZE = 28;
    const TOPIC_AUTHORIZATION_FAILED = 29;
    const GROUP_AUTHORIZATION_FAILED = 30;
    const UNSUPPORTED_FOR_MESSAGE_FORMAT = 43;
    const UNKNOWN = -1;

    // }}}
    // {{{ members
    
    private static $instance = null;

    private $callStatus = array();
    
    private $requests  = array(
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
            'interval' => 2000,
        ),
    );

    private $consumer = null;

    // }}}
    // {{{ functions
    // {{{ public function static getInstance()

    /**
     * set send messages
     *
     * @access public
     * @param $hostList
     * @param null $timeout
     * @return Consumer
     */
    public static function getInstance()
    {
        if (is_null(self::$instance)) {
            self::$instance = new self();
        }

        return self::$instance;
    }

    // }}}
    // {{{ private function __construct()

    /**
     * __construct
     *
     * @access public
     * @param $hostList
     * @param null $timeout
     */
    private function __construct()
    {
    }

    // }}}
    
    // {{{ public function setOnConsumer()

    public function setOnConsumer($consumer)
    {
        $this->consumer = $consumer; 
    }

    // }}}
    
    // {{{ public function waitSyncMeta()

    public function waitSyncMeta()
    {
        $this->debug('Start sync metadata request');
        $brokerList = explode(',', \Kafka\ConsumerConfig::getInstance()->getMetadataBrokerList());
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
        $connections = \Kafka\Consumer\Connection::getInstance();
        foreach ($brokerHost as $host) {
            $socket = $connections->getMetaConnect($host);
            if ($socket) {
                $meta = new \Kafka\Protocol\Metadata(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
                $topics = \Kafka\ConsumerConfig::getInstance()->getTopics();
                $requestData = $meta->encode($topics);
                $this->debug('Start sync metadata request params:' . json_encode($topics));
                $socket->write($requestData);
                return;
            }
        }
        throw new \Kafka\Exception('Not has broker can connection `metadataBrokerList`');
    }

    // }}}
    // {{{ public function failSyncMeta()

    public function failSyncMeta()
    {
        $this->error('Start sync metadata request fail, errorCode');
    }

    // }}}
    // {{{ public function succSyncMeta()

    public function succSyncMeta($brokerResult, $topics)
    {
        $brokers = array();
        $connections = \Kafka\Consumer\Connection::getInstance();
        foreach ($brokerResult as $value) {
            $key = $value['nodeId'];
            $hostname = $value['host'] . ':' . $value['port'];
            $brokers[$key] = $hostname;
        }

        $oldBrokers = $connections->getBrokers();
        $needChange = false;
        if (serialize($oldBrokers) != serialize($brokers)) {
            $needChange = true;
        }

        $broker = \Kafka\Consumer\Broker::getInstance();
        $oldTopics = $broker->getTopics();
        $newTopics = array();
        foreach ($topics as $topic) {
            if ($topic['errorCode'] != 0) {
                continue;
            }
            $item = array();
            foreach ($topic['partitions']  as $part) {
                $item[$part['partitionId']] = $part['leader'];
            }
            $newTopics[$topic['topicName']] = $item;
        }
        if (serialize($oldTopics) != serialize($newTopics)) {
            $broker->setTopics($newTopics);
            $needChange = true;
        }

        if ($needChange) {
            $this->onBrokerChange($brokers);
        }
    }

    // }}}
    
    // {{{ public function getGroupBrokerId()

    public function getGroupBrokerId()
    {
        $connections = \Kafka\Consumer\Connection::getInstance();
        $connect = $connections->getRandConnect();
        if (!$connect) {
            return;
        }
        $group = new \Kafka\Protocol\GroupCoordinator(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
        $requestData = $group->encode(array(
            'group_id' => \Kafka\ConsumerConfig::getInstance()->getGroupId(),
        ));
        $connect->write($requestData);
    }

    // }}}
    // {{{ public function failGetGroupBrokerId()

    public function failGetGroupBrokerId($errorCode)
    {
        $this->error("Get group broker fail, errorCode:" . $errorCode);
    }

    // }}}
    // {{{ public function succGetGroupBrokerId()

    public function succGetGroupBrokerId($result)
    {
        \Kafka\Consumer\Broker::getInstance()->setGroupBrokerId($result);
        $this->stopItem(self::REQUEST_GETGROUP);
        $this->debug("Get group broker id:" . $result);
        $this->runItem(self::REQUEST_JOINGROUP);
    }

    // }}}
    
    // {{{ public function joinGroup()

    public function joinGroup()
    {
        $groupBrokerId = \Kafka\Consumer\Broker::getInstance()->getGroupBrokerId();
        $connections = \Kafka\Consumer\Connection::getInstance();
        $connect = $connections->getMetaConnect($groupBrokerId);
        if (!$connect) {
            return;
        }
        $groupJoin = new \Kafka\Protocol\JoinGroup(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
        $topics = \Kafka\ConsumerConfig::getInstance()->getTopics();
        $assign = \Kafka\Consumer\Assignment::getInstance();
        $memberId = $assign->getMemberId();
        $params = array(
            'group_id' => \Kafka\ConsumerConfig::getInstance()->getGroupId(),
            'session_timeout' => \Kafka\ConsumerConfig::getInstance()->getSessionTimeout(),
            'rebalance_timeout' => \Kafka\ConsumerConfig::getInstance()->getRebalanceTimeout(),
            'member_id' => ($memberId == null) ? '' : $memberId,
            'data' => array(
                array(
                    'protocol_name' => 'range',
                    'version' => 0,
                    'subscription' => $topics,
                    'user_data' => '',
                ),
            ),
        );
        $requestData = $groupJoin->encode($params);
        $connect->write($requestData);
        $this->debug("Join group start, params:" . json_encode($params));
    }

    // }}}
    // {{{ public function failJoinGroup()

    public function failJoinGroup($errorCode)
    {
        $this->stopItem(self::REQUEST_JOINGROUP);
        $assign = \Kafka\Consumer\Assignment::getInstance();
        $memberId = $assign->getMemberId();
        $error = sprintf('Join group fail, need rejoin, errorCode %d, memberId: %s', $errorCode, $memberId);
        $this->error($error);
        $this->stateConvert($errorCode);
    }

    // }}}
    // {{{ public function succJoinGroup()

    public function succJoinGroup($result)
    {
        $assign = \Kafka\Consumer\Assignment::getInstance();
        $assign->setMemberId($result['memberId']);
        $assign->setGenerationId($result['generationId']);
        if ($result['leaderId'] == $result['memberId']) { // leader assign partition
            $assigns = $assign->assign($result['members']);
        }
        $msg = sprintf('Join group sucess, params: %s', json_encode($result));
        $this->debug($msg);

        $this->stopItem(self::REQUEST_JOINGROUP);
        $this->restartItem(self::REQUEST_SYNCGROUP);
    }

    // }}}
    
    // {{{ public function syncGroup()

    public function syncGroup()
    {
        $groupBrokerId = \Kafka\Consumer\Broker::getInstance()->getGroupBrokerId();
        $connections = \Kafka\Consumer\Connection::getInstance();
        $connect = $connections->getMetaConnect($groupBrokerId);
        if (!$connect) {
            return;
        }
        $groupSync = new \Kafka\Protocol\SyncGroup(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
        $topics = \Kafka\ConsumerConfig::getInstance()->getTopics();
        $assign = \Kafka\Consumer\Assignment::getInstance();
        $memberId = $assign->getMemberId();
        $generationId = $assign->getGenerationId();
        $params = array(
            'group_id' => \Kafka\ConsumerConfig::getInstance()->getGroupId(),
            'generation_id' => $generationId,
            'member_id' => $memberId,
            'data' => $assign->getAssignments(),
        );
        $requestData = $groupSync->encode($params);
        $this->debug("Sync group start, params:" . json_encode($params));
        $connect->write($requestData);
    }

    // }}}
    // {{{ public function failSyncGroup()

    public function failSyncGroup($errorCode)
    {
        $this->stopItem(self::REQUEST_SYNCGROUP);
        $error = sprintf('Sync group fail, need rejoin, errorCode %d, memberId: %s', $errorCode, $memberId);
        $this->error($error);
        $this->stateConvert($errorCode);
    }

    // }}}
    // {{{ public function succSyncGroup()

    public function succSyncGroup($result)
    {
        $msg = sprintf('Sync group sucess, params: %s', json_encode($result));
        $this->debug($msg);

        $topics = \Kafka\Consumer\Broker::getInstance()->getTopics();
        $brokerToTopics = array();
        foreach ($result['partitionAssignments'] as $topic) {  
            foreach ($topic['partitions'] as $partId) {
                $brokerId = $topics[$topic['topicName']][$partId];
                if (!isset($brokerToTopics[$brokerId])) {
                    $brokerToTopics[$brokerId] = array();
                }

                $topicInfo = array();
                if (isset($brokerToTopics[$brokerId][$topic['topicName']])) {
                    $topicInfo = $brokerToTopics[$brokerId][$topic['topicName']];
                }
                $topicInfo['topic_name'] = $topic['topicName'];
                if (!isset($topicInfo['partitions'])) {
                    $topicInfo['partitions'] = array();
                }
                $topicInfo['partitions'][] = $partId;
                $brokerToTopics[$brokerId][$topic['topicName']] = $topicInfo;
            }
        }
        $assign = \Kafka\Consumer\Assignment::getInstance();
        $assign->setTopics($brokerToTopics);

        $this->stopItem(self::REQUEST_SYNCGROUP);
        $this->restartItem(self::REQUEST_HEARTGROUP);
        $this->runItem(self::REQUEST_OFFSET);
        $this->runItem(self::REQUEST_FETCH_OFFSET);
    }

    // }}}
    
    // {{{ protected function heartbeat()

    protected function heartbeat()
    {
        $groupBrokerId = \Kafka\Consumer\Broker::getInstance()->getGroupBrokerId();
        $connections = \Kafka\Consumer\Connection::getInstance();
        $connect = $connections->getMetaConnect($groupBrokerId);
        if (!$connect) {
            return;
        }
        $heartbeat = new \Kafka\Protocol\Heartbeat(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
        $assign = \Kafka\Consumer\Assignment::getInstance();
        $memberId = $assign->getMemberId();
        if (!$memberId) {
            return;
        }
        $generationId = $assign->getGenerationId();
        $params = array(
            'group_id' => \Kafka\ConsumerConfig::getInstance()->getGroupId(),
            'generation_id' => $generationId,
            'member_id' => $memberId,
        );
        $requestData = $heartbeat->encode($params);
        //$this->debug("Heartbeat group start, params:" . json_encode($params));
        $connect->write($requestData);
}

// }}}
    // {{{ public function failHeartbeat()

    public function failHeartbeat($errorCode)
    {
        // heartbeat request must after has join group and sync group
        $this->stopItem(self::REQUEST_HEARTGROUP);
        $this->error('Heartbeat error, errorCode:' . $errorCode);
        $this->stateConvert($errorCode);
    }

    // }}}
    // {{{ public function succHeartbeat()

    public function succHeartbeat($result)
    {
        $this->restartItem(self::REQUEST_HEARTGROUP);
    }

    // }}}
    
    // {{{ protected function fetch()

    protected function fetch()
    {
        $connections = \Kafka\Consumer\Connection::getInstance();
        $topics = \Kafka\Consumer\Assignment::getInstance()->getTopics();
        $consumerOffsets = \Kafka\Consumer\Assignment::getInstance()->getConsumerOffsets();
        foreach ($topics as $brokerId => $topicList) {
            $connect = $connections->getMetaConnect($brokerId);
            if (!$connect) {
                return;
            }

            $data = array();
            $fetch = new \Kafka\Protocol\Fetch(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
            foreach ($topicList as $topic) {
                $item = array(
                    'topic_name' => $topic['topic_name'],
                    'partitions' => array(),
                );
                foreach ($topic['partitions'] as $partId) {
                    $item['partitions'][] = array(
                        'partition_id' => $partId,
                        'offset' => isset($consumerOffsets[$topic['topic_name']][$partId]) ? $consumerOffsets[$topic['topic_name']][$partId] : 0,
                        'max_bytes' => 1024 * 1024 * 2,
                    );
                }
                $data[] = $item;
            }
            $params = array(
                'max_wait_time' => 100,
                'replica_id' => -1,
                'min_bytes' => '1000',
                'data' => $data,
            );
            $this->debug("Fetch message start, params:" . json_encode($params));
            $requestData = $fetch->encode($params);
            $connect->write($requestData);
        }
    }

    // }}}
    // {{{ public function succFetch()

    public function succFetch($result)
    {
        $assign = \Kafka\Consumer\Assignment::getInstance();
        //$this->debug('Fetch success, result:' . json_encode($result));
        foreach ($result['topics'] as $topic) {
            foreach ($topic['partitions'] as $part) {
                $context = array(
                    $topic['topicName'],
                    $part['partition'],
                );
                if ($part['errorCode'] != 0) {
                    $this->stateConvert($part['errorCode'], $context);
                    continue;
                }

                $offset = $assign->getConsumerOffset($topic['topicName'], $part['partition']);
                foreach ($part['messages'] as $message) {
                    if ($this->consumer != null) {
                        call_user_func($this->consumer, $topic['topicName'], $part['partition'], $message);
                    }
                    $offset = $message['offset'];
                }
                $assign->setConsumerOffset($topic['topicName'], $part['partition'], $offset + 1);
                $assign->setCommitOffset($topic['topicName'], $part['partition'], $offset);
            }
        }
        $this->restartItem(self::REQUEST_FETCH);
    }

    // }}}
    
    // {{{ protected function commit()

    protected function commit()
    {
        $groupBrokerId = \Kafka\Consumer\Broker::getInstance()->getGroupBrokerId();
        $connections = \Kafka\Consumer\Connection::getInstance();
        $connect = $connections->getMetaConnect($groupBrokerId);
        if (!$connect) {
            return;
        }

        $commit = new \Kafka\Protocol\CommitOffset(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
        $commitOffsets = \Kafka\Consumer\Assignment::getInstance()->getCommitOffsets();
        $topics = \Kafka\Consumer\Assignment::getInstance()->getTopics();
        \Kafka\Consumer\Assignment::getInstance()->setPrecommitOffsets($commitOffsets);
        $data = array();
        foreach ($topics as $brokerId => $topicList) {
            foreach ($topicList as $topic) {
                $partitions = array();
                if (isset($data[$topic['topic_name']]['partitions'])) {
                    $partitions = $data[$topic['topic_name']]['partitions'];
                }
                foreach ($topic['partitions'] as $partId) {
                    if ($commitOffsets[$topic['topic_name']][$partId] == -1) {
                        continue;
                    }
                    $partitions[$partId]['partition'] = $partId;
                    $partitions[$partId]['offset'] = $commitOffsets[$topic['topic_name']][$partId];
                }
                $data[$topic['topic_name']]['partitions'] = $partitions;
                $data[$topic['topic_name']]['topic_name'] = $topic['topic_name'];
            }
        }
        $params = array(
            'group_id' => \Kafka\ConsumerConfig::getInstance()->getGroupId(),
            'generation_id' => \Kafka\Consumer\Assignment::getInstance()->getGenerationId(),
            'member_id' => \Kafka\Consumer\Assignment::getInstance()->getMemberId(),
            'data' => $data,
        );
        $this->debug("Commit current fetch offset start, params:" . json_encode($params));
        $requestData = $commit->encode($params);
        $connect->write($requestData);
    }

    // }}}
    // {{{ public function succCommit()

    public function succCommit($result)
    {
        $this->debug('Commit success, result:' . json_encode($result));
        $this->restartItem(self::REQUEST_COMMIT_OFFSET);
    }

    // }}}
    
    // {{{ protected function offset()

    protected function offset()
    {
        $connections = \Kafka\Consumer\Connection::getInstance();
        $topics = \Kafka\Consumer\Assignment::getInstance()->getTopics();
        foreach ($topics as $brokerId => $topicList) {
            $connect = $connections->getMetaConnect($brokerId);
            if (!$connect) {
                return;
            }
            $offset = new \Kafka\Protocol\Offset(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
            $data = array();
            $resetOffset = \Kafka\ConsumerConfig::getInstance()->getOffsetReset();
            foreach ($topicList as $topic) {
                $item = array(
                    'topic_name' => $topic['topic_name'],
                    'partitions' => array(),
                );
                foreach ($topic['partitions'] as $partId) {
                    $item['partitions'][] = array(
                        'partition_id' => $partId,
                        'offset' => 1,
                        'time' =>  ($resetOffset == 'latest') ? -1 : -2,
                    );
                    $data[] = $item;
                }
            }
            $params = array(
                'replica_id' => -1,
                'data' => $data,
            );
            //$this->debug("Get current offset start, params:" . json_encode($params));
            $requestData = $offset->encode($params);
            $connect->write($requestData);
        }
    }

    // }}}
    // {{{ public function succOffset()

    public function succOffset($result)
    {
        $msg = sprintf('Get current offset sucess, result: %s', json_encode($result));
        //$this->debug($msg);

        $offsets = \Kafka\Consumer\Assignment::getInstance()->getOffsets();
        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] != 0) {
                    $this->stateConvert($part['errorCode']);
                    break 2;
                } 

                $offsets[$topic['topicName']][$part['partition']] = $part['offsets'][0];
            }
        }
        \Kafka\Consumer\Assignment::getInstance()->setOffsets($offsets);
        $this->restartItem(self::REQUEST_OFFSET);
    }

    // }}}
    
    // {{{ protected function fetchOffset()

    protected function fetchOffset()
    {
        $groupBrokerId = \Kafka\Consumer\Broker::getInstance()->getGroupBrokerId();
        $connections = \Kafka\Consumer\Connection::getInstance();
        $connect = $connections->getMetaConnect($groupBrokerId);
        if (!$connect) {
            return;
        }

        $offset = new \Kafka\Protocol\FetchOffset(\Kafka\ConsumerConfig::getInstance()->getBrokerVersion());
        $topics = \Kafka\Consumer\Assignment::getInstance()->getTopics();
        $data = array();
        foreach ($topics as $brokerId => $topicList) {
            foreach ($topicList as $topic) {
                $partitions = array();
                if (isset($data[$topic['topic_name']]['partitions'])) {
                    $partitions = $data[$topic['topic_name']]['partitions'];
                }
                foreach ($topic['partitions'] as $partId) {
                    $partitions[] = $partId;
                }
                $data[$topic['topic_name']]['partitions'] = $partitions;
                $data[$topic['topic_name']]['topic_name'] = $topic['topic_name'];
            }
        }
        $params = array(
            'group_id' => \Kafka\ConsumerConfig::getInstance()->getGroupId(),
            'data' => $data,
        );
        //$this->debug("Get current fetch offset start, params:" . json_encode($params));
        $requestData = $offset->encode($params);
        $connect->write($requestData);
    }

    // }}}
    // {{{ public function succFetchOffset()

    public function succFetchOffset($result)
    {
        $msg = sprintf('Get current fetch offset sucess, result: %s', json_encode($result));
        //$this->debug($msg);

        $assign = \Kafka\Consumer\Assignment::getInstance();
        $offsets = $assign->getFetchOffsets();
        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] != 0) {
                    $this->stateConvert($part['errorCode']);
                    break 2;
                } 

                $offsets[$topic['topicName']][$part['partition']] = $part['offset'];
            }
        }
        $assign->setFetchOffsets($offsets);

        $consumerOffsets = $assign->getConsumerOffsets();
        if (empty($consumerOffsets)) {
            $assign->setConsumerOffsets($assign->getFetchOffsets());
            $assign->setCommitOffsets($assign->getFetchOffsets());
            // first start fetch request , must in fetch offset request after
            $this->runItem(self::REQUEST_FETCH);
            $this->runItem(self::REQUEST_COMMIT_OFFSET);
        }

        $this->restartItem(self::REQUEST_FETCH_OFFSET);
    }

    // }}}
    
    // {{{ protected function onBrokerChange()

    protected function onBrokerChange($brokers)
    {
        $this->stop();
        $connections = \Kafka\Consumer\Connection::getInstance();
        $connections->setBrokers($brokers);
        $this->init();
        $this->start();
        $this->error('Broker is change, kafka consumer will resume...');
    }

    // }}}
    
    // {{{ protected function isProcess()

    protected function isProcess($request)
    {
        return $this->callStatus[$request]['status'] == self::STATUS_PROCESS; 
    }

    // }}}
    // {{{ protected function runItem()

    protected function runItem($request, $try = false)
    {
        if (!$this->isProcess($request) || $try) {
            $this->callStatus[$request]['status'] = self::STATUS_START;
        }  
    }

    // }}}
    // {{{ protected function restartItem()

    protected function restartItem($request)
    {
        $this->callStatus[$request]['status'] = self::STATUS_START;
    }

    // }}}
    // {{{ protected function stopItem()

    protected function stopItem($request)
    {
        $this->callStatus[$request]['status'] = self::STATUS_STOP;
    }

    // }}}
    // {{{ protected function start()

    protected function start() 
    {
        foreach ($this->requests as $request => $option) {
            $interval = isset($option['interval']) ? $option['interval'] : 200;
            \Amp\repeat(function ($watcherId) use($request) {
                if ($this->callStatus[$request]['status'] == self::STATUS_START
                    && $this->callStatus[$request]['func'] != null) {
                    call_user_func($this->callStatus[$request]['func']);
                    $this->callStatus[$request]['status'] = self::STATUS_PROCESS;
                }
                $this->callStatus[$request]['watcher'] = $watcherId;
            }, $msInterval = $interval);
        }

        \Amp\repeat(function ($watcherId) use($request) {
            foreach ($this->callStatus as $key => $status) {
                var_dump($key  . ':' .$status['status']); 
            }
        }, $msInterval = 2000);
        $this->runItem(self::REQUEST_GETGROUP);
    }

    // }}}
    // {{{ protected function init()

    protected function init() 
    {
        foreach ($this->requests as $request => $option) {
            if (!isset($this->callStatus[$request])) {
                $this->callStatus[$request] = array(
                    'func'   => null,
                    'status' => self::STATUS_STOP,
                    'watcher' => 0,
                    'time'    => 0,
                );
            }
        }
        $this->callStatus[self::REQUEST_GETGROUP]['func'] = function() {
            $this->getGroupBrokerId();
        };
        $this->callStatus[self::REQUEST_JOINGROUP]['func'] = function() {
            $this->joinGroup();
        };
        $this->callStatus[self::REQUEST_SYNCGROUP]['func'] = function() {
            $this->syncGroup();
        };
        $this->callStatus[self::REQUEST_HEARTGROUP]['func'] = function() {
            $this->heartbeat();
        };
        $this->callStatus[self::REQUEST_OFFSET]['func'] = function() {
            $this->offset();
        };
        $this->callStatus[self::REQUEST_FETCH_OFFSET]['func'] = function() {
            $this->fetchOffset();
        };
        $this->callStatus[self::REQUEST_FETCH]['func'] = function() {
            $this->fetch();
        };
        $this->callStatus[self::REQUEST_COMMIT_OFFSET]['func'] = function() {
            $this->commit();
        };
    }

    // }}}
    // {{{ protected function stop()

    protected function stop()
    {
        foreach($this->callStatus as $key => $value) {
            \Amp\cancel($value['watcher']);
            $this->stopItem($key);
        }

        // stop fetch

        $connections = \Kafka\Consumer\Connection::getInstance();
        // close all broker connect
        $connections->setBrokers(array());
    }

    // }}}
    // {{{ protected function stateConvert()

    protected function stateConvert($errorCode, $context = null)
    {
        switch($errorCode) {
            case self::OFFSET_OUT_OF_RANGE:
                $assign = \Kafka\Consumer\Assignment::getInstance();
                $offsets = $assign->getOffsets();
                list($topic, $partId) = $context;
                if (isset($offsets[$topic][$partId])) {
                    $assign->setConsumerOffset($topic, $partId, $offsets[$topic][$partId]);
                }
                break;
            case self::UNKNOWN_TOPIC_OR_PARTITION:
                $this->error('Unknown topic or partition.');
                //throw new \Kafka\Exception('Unknown topic or partition');
                break;
            case self::UNSUPPORTED_FOR_MESSAGE_FORMAT:
                $this->error('Unsupported message, broker version need > 0.9.0.0');
                //throw new \Kafka\Exception('Unknown topic or partition');
                break;
            case self::NOT_LEADER_FOR_PARTITION:
            case self::UNKNOWN:
                $this->waitSyncMeta();
                break;
            case self::GROUP_LOAD_IN_PROGRESS:
                if (!$this->isProcess(self::REQUEST_JOINGROUP)) {
                    $this->runItem(self::REQUEST_JOINGROUP);      
                    $this->stopItem(self::REQUEST_SYNCGROUP);      
                } else {
                    $this->error('Join group processing...');
                }
                break;
            case self::NOT_COORDINATOR_FOR_GROUP:
                $this->runItem(self::REQUEST_GETGROUP);
                $this->stopItem(self::REQUEST_JOINGROUP);
                $this->stopItem(self::REQUEST_SYNCGROUP);
                $this->stopItem(self::REQUEST_HEARTGROUP);
                break;
            case self::ILLEGAL_GENERATION:
            case self::REBALANCE_IN_PROGRESS:
                if (!$this->isProcess(self::REQUEST_JOINGROUP)) {
                    $this->runItem(self::REQUEST_JOINGROUP);
                } else {
                    $this->error('Join group processing...');
                }
                $this->stopItem(self::REQUEST_SYNCGROUP);
                break;
            case self::UNKNOWN_MEMBER_ID:
                $assign = \Kafka\Consumer\Assignment::getInstance();
                if (!$this->isProcess(self::REQUEST_JOINGROUP)) {
                    $assign->setMemberId('');
                    $this->runItem(self::REQUEST_JOINGROUP);
                } else {
                    $this->error('Join group processing...');
                }
                $this->stopItem(self::REQUEST_SYNCGROUP);
                break;
        } 
    }

    // }}}
    // }}}
}
