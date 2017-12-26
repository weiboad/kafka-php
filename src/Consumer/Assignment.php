<?php
namespace Kafka\Consumer;

use Kafka\SingletonTrait;
use Kafka\Broker;

class Assignment
{
    use SingletonTrait;
    
    private $memberId = '';

    private $generationId = '';

    private $assignments = [];

    private $topics = [];

    private $offsets = [];

    private $lastOffsets = [];

    private $fetchOffsets = [];

    private $consumerOffsets = [];

    private $commitOffsets = [];

    private $precommitOffsets = [];

    public function setMemberId($memberId)
    {
        $this->memberId = $memberId;
    }

    public function getMemberId()
    {
        return $this->memberId;
    }

    public function setGenerationId($generationId)
    {
        $this->generationId = $generationId;
    }

    public function getGenerationId()
    {
        return $this->generationId;
    }

    public function getAssignments()
    {
        return $this->assignments;
    }

    public function assign($result)
    {
        $broker = Broker::getInstance();
        $topics = $broker->getTopics();

        $memberCount = count($result);

        $count   = 0;
        $members = [];
        foreach ($topics as $topicName => $partition) {
            foreach ($partition as $partId => $leaderId) {
                $memberNum = $count % $memberCount;
                if (! isset($members[$memberNum])) {
                    $members[$memberNum] = [];
                }
                if (! isset($members[$memberNum][$topicName])) {
                    $members[$memberNum][$topicName] = [];
                }
                $members[$memberNum][$topicName]['topic_name'] = $topicName;
                if (! isset($members[$memberNum][$topicName]['partitions'])) {
                    $members[$memberNum][$topicName]['partitions'] = [];
                }
                $members[$memberNum][$topicName]['partitions'][] = $partId;
                $count++;
            }
        }

        $data = [];
        foreach ($result as $key => $member) {
            $item   = [
                'version' => 0,
                'member_id' => $member['memberId'],
                'assignments' => isset($members[$key]) ? $members[$key] : [],
            ];
            $data[] = $item;
        }
        $this->assignments = $data;
    }

    public function setTopics($topics)
    {
        $this->topics = $topics;
    }

    public function getTopics()
    {
        return $this->topics;
    }

    public function setOffsets($offsets)
    {
        $this->offsets = $offsets;
    }

    public function getOffsets()
    {
        return $this->offsets;
    }

    public function setLastOffsets($offsets)
    {
        $this->lastOffsets = $offsets;
    }

    public function getLastOffsets()
    {
        return $this->lastOffsets;
    }

    public function setFetchOffsets($offsets)
    {
        $this->fetchOffsets = $offsets;
    }

    public function getFetchOffsets()
    {
        return $this->fetchOffsets;
    }

    public function setConsumerOffsets($offsets)
    {
        $this->consumerOffsets = $offsets;
    }

    public function getConsumerOffsets()
    {
        return $this->consumerOffsets;
    }

    public function setConsumerOffset($topic, $part, $offset)
    {
        $this->consumerOffsets[$topic][$part] = $offset;
    }

    public function getConsumerOffset($topic, $part)
    {
        if (! isset($this->consumerOffsets[$topic][$part])) {
            return false;
        }
        return $this->consumerOffsets[$topic][$part];
    }

    public function setCommitOffsets($offsets)
    {
        $this->commitOffsets = $offsets;
    }

    public function getCommitOffsets()
    {
        return $this->commitOffsets;
    }

    public function setCommitOffset($topic, $part, $offset)
    {
        $this->commitOffsets[$topic][$part] = $offset;
    }

    public function setPrecommitOffsets($offsets)
    {
        $this->precommitOffsets = $offsets;
    }

    public function getPrecommitOffsets()
    {
        return $this->precommitOffsets;
    }

    public function setPrecommitOffset($topic, $part, $offset)
    {
        $this->precommitOffsets[$topic][$part] = $offset;
    }

    public function clearOffset()
    {
        $this->offsets          = [];
        $this->lastOffsets      = [];
        $this->fetchOffsets     = [];
        $this->consumerOffsets  = [];
        $this->commitOffsets    = [];
        $this->precommitOffsets = [];
    }
}
