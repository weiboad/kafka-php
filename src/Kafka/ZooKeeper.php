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
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class ZooKeeper implements \Kafka\ClusterMetaData
{
    // {{{ consts

    /**
     * get all broker
     */
    const BROKER_PATH = '/brokers/ids';

    /**
     * get broker detail
     */
    const BROKER_DETAIL_PATH = '/brokers/ids/%d';

    /**
     * get topic detail
     */
    const TOPIC_PATCH = '/brokers/topics/%s';

    /**
     * get partition state
     */
    const PARTITION_STATE = '/brokers/topics/%s/partitions/%d/state';

    /**
     * register consumer
     */
    const REG_CONSUMER = '/consumers/%s/ids';

    /**
     * list consumer
     */
    const LIST_CONSUMER = '/consumers/%s/ids';

    /**
     * partition owner
     */
    const PARTITION_OWNER = '/consumers/%s/owners/%s';

    // }}}
    // {{{ members

    /**
     * zookeeper
     *
     * @var mixed
     * @access private
     */
    private $zookeeper = null;

    /**
     * @var Callback container
     */
    private $callback = array();

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    /**
     * Cached list of all kafka brokers
     *
     * @var array
     * @access private
     */
    private $brokers = array();

    /**
     * __construct
     *
     * @access public
     * @param $hostList
     * @param null $timeout
     */
    public function __construct($hostList, $timeout = null)
    {
        if (!is_null($timeout) && is_numeric($timeout)) {
            $this->zookeeper = new \ZooKeeper($hostList, null, $timeout);
        } else {
            $this->zookeeper = new \ZooKeeper($hostList);
        }
    }

    // }}}
    // {{{ public function listBrokers()

    /**
     * get broker list using zookeeper
     *
     * @access public
     * @return array
     */
    public function listBrokers()
    {
        // If broker cache hasn't been populated
        if (count($this->brokers) == 0) {
            // Populate broker cache
            $result = array();
            $lists = $this->zookeeper->getChildren(self::BROKER_PATH);
            if (!empty($lists)) {
                foreach ($lists as $brokerId) {
                    $brokerDetail = $this->getBrokerDetail($brokerId);
                    if (!$brokerDetail) {
                        continue;
                    }
                    $result[$brokerId] = $brokerDetail;
                }
            }
            $this->brokers = $result;
        }
        return $this->brokers;
    }

    // }}}
    // {{{ public function getBrokerDetail()

    /**
     * get broker detail
     *
     * @param integer $brokerId
     * @access public
     * @return string|bool
     */
    public function getBrokerDetail($brokerId)
    {
        $result = array();
        $path = sprintf(self::BROKER_DETAIL_PATH, (int) $brokerId);
        if ($this->zookeeper->exists($path)) {
            $result = $this->zookeeper->get($path);
            if (!$result) {
                return false;
            }

            $result = json_decode($result, true);
        }

        return $result;
    }

    // }}}
    // {{{ public function getTopicDetail()

    /**
     * get topic detail
     *
     * @param string $topicName
     * @access public
     * @return string|bool
     */
    public function getTopicDetail($topicName)
    {
        $result = array();
        $path = sprintf(self::TOPIC_PATCH, (string) $topicName);
        if ($this->zookeeper->exists($path)) {
            $result = $this->zookeeper->get($path);
            if (!$result) {
                return false;
            }
            $result = json_decode($result, true);
        }

        return $result;
    }

    // }}}
    // {{{ public function getPartitionState()

    /**
     * get partition state
     *
     * @param string $topicName
     * @param integer $partitionId
     * @access public
     * @return string|bool
     */
    public function getPartitionState($topicName, $partitionId = 0)
    {
        $result = array();
        $path = sprintf(self::PARTITION_STATE, (string) $topicName, (int) $partitionId);
        if ($this->zookeeper->exists($path)) {
            $result = $this->zookeeper->get($path);
            if (!$result) {
                return false;
            }
            $result = json_decode($result, true);
        }

        return $result;
    }

    // }}}
    // {{{ public function registerConsumer()

    /**
     * register consumer
     *
     * @param $groupId
     * @param integer $consumerId
     * @param array $topics
     * @access public
     */
    public function registerConsumer($groupId, $consumerId, $topics = array())
    {
        if (empty($topics)) {
            return;
        }

        $path = sprintf(self::REG_CONSUMER, (string) $groupId);
        $subData = array();
        foreach ($topics as $topic) {
            $subData[$topic] = 1;
        }
        $data = array(
            'version' => '1',
            'pattern' => 'white_list',
            'subscription' => $subData,
        );
        if (!$this->zookeeper->exists($path)) {
            $this->makeZkPath($path);
        }
        $consumerPath = $path . '/' . $consumerId;
        if (!$this->zookeeper->exists($consumerPath)) {
            $this->makeZkPath($consumerPath);
            $this->makeZkNode($consumerPath, json_encode($data), \ZooKeeper::EPHEMERAL);
        } else {
            $this->zookeeper->set($consumerPath, json_encode($data));
        }
//var_dump($this->zookeeper->getChildren($path));
    }

    // }}}
    // {{{ public function listConsumer()

    /**
     * list consumer
     *
     * @param string $groupId
     * @access public
     * @return array
     */
    public function listConsumer($groupId)
    {
        $path = sprintf(self::LIST_CONSUMER, (string) $groupId);
        return $this->zookeeper->getChildren($path);
    }

    // }}}
    // {{{ public function getConsumersPerTopic()

    /**
     * get consumer per topic
     *
     * @param string $groupId
     * @access public
     * @return array
     */
    public function getConsumersPerTopic($groupId)
    {
        $consumers = $this->listConsumer($groupId);
        if (empty($consumers)) {
            return array();
        }

        $topics = array();
        foreach ($consumers as $consumerId) {
            $path = sprintf(self::REG_CONSUMER, (string) $groupId) . '/' . $consumerId;
            if (!$this->zookeeper->exists($path)) {
                continue;
            }

            $info = $this->zookeeper->get($path);
            $info = json_decode($info, true);
            $subTopic = isset($info['subscription']) ? $info['subscription'] : array();
            foreach ($subTopic as $topic => $num) {
                $topics[$topic] = $consumerId;
            }
        }

        return $topics;
    }

    // }}}
    // {{{ public function addPartitionOwner()

    /**
     * add partition owner
     *
     * @param string $groupId
     * @param string $topicName
     * @param integer $partitionId
     * @param string $consumerId
     * @access public
     * @return void
     */
    public function addPartitionOwner($groupId, $topicName, $partitionId, $consumerId)
    {
        $path = sprintf(self::PARTITION_OWNER, (string) $groupId, $topicName);
        if (!$this->zookeeper->exists($path)) {
            $this->makeZkPath($path);
        }
        $partitionPath = $path . '/' . $partitionId;
        if (!$this->zookeeper->exists($partitionPath)) {
            $this->makeZkPath($partitionPath);
            $this->makeZkNode($partitionPath, $consumerId, \ZooKeeper::EPHEMERAL);
        }

        // if exists path other comsumer
    }

    // }}}
    // {{{ protected function makeZkPath()

    /**
     * Equivalent of "mkdir -p" on ZooKeeper
     *
     * @param string $path  The path to the node
     * @param mixed  $value The value to assign to each new node along the path
     *
     * @return bool
     */
    protected function makeZkPath($path, $value = 0)
    {
        $parts = explode('/', $path);
        $parts = array_filter($parts);
        $subpath = '';
        while (count($parts) >= 1) {
            $subpath .= '/' . array_shift($parts);
            if (!$this->zookeeper->exists($subpath)) {
                $this->makeZkNode($subpath, $value);
            }
        }
    }

    // }}}
    // {{{ protected function makeZkNode()

    /**
     * Create a node on ZooKeeper at the given path
     *
     * @param string $path  The path to the node
     * @param mixed  $value The value to assign to the new node
     *
     * @return bool
     */
    protected function makeZkNode($path, $value, $flag = null)
    {
        $params = array(
            array(
                'perms'  => \Zookeeper::PERM_ALL,
                'scheme' => 'world',
                'id'     => 'anyone',
            )
        );
        return $this->zookeeper->create($path, $value, $params, $flag);
    }

    // }}}
    // {{{ public function watch()

    /**
     * Wath a given path
     * @param string $path the path to node
     * @param callable $callback callback function
     * @return string|null
     */
    public function watch($path, $callback)
    {
        if (!is_callable($callback)) {
            return null;
        }

        if ($this->zookeeper->exists($path)) {
            if (!isset($this->callback[$path])) {
                $this->callback[$path] = array();
            }
            if (!in_array($callback, $this->callback[$path])) {
                $this->callback[$path][] = $callback;
                return $this->zookeeper->getChildren($path, array($this, 'watchCallback'));
            }
        }
    }

    // }}}
    // {{{ public function watchCallback()

    /**
     * Wath event callback warper
     * @param int $event_type
     * @param int $stat
     * @param string $path
     * @return the return of the callback or null
     */
    public function watchCallback($event_type, $stat, $path)
    {
        if (!isset($this->callback[$path])) {
            return null;
        }

        foreach ($this->callback[$path] as $callback) {
            $this->zookeeper->getChildren($path, array($this, 'watchCallback'));
            return call_user_func($callback);
        }
    }

    // }}}
    // {{{ public function cancelWatch()

    /**
     * Delete watch callback on a node, delete all callback when $callback is null
     * @param string $path
     * @param callable $callback
     * @return boolean|NULL
     */
    public function cancelWatch($path, $callback = null)
    {
        if (isset($this->callback[$path])) {
            if (empty($callback)) {
                unset($this->callback[$path]);
                $this->zookeeper->get($path); //reset the callback
                return true;
            } else {
                $key = array_search($callback, $this->callback[$path]);
                if ($key !== false) {
                    unset($this->callback[$path][$key]);
                    return true;
                } else {
                    return null;
                }
            }
        } else {
            return null;
        }
    }

    // }}}
    // {{{ public function refreshMetadata()

    /**
     * Clear internal caches
     * @return null
     */
    public function refreshMetadata()
    {
        $this->brokers = array();
    }

    // }}}
    // }}}
}
