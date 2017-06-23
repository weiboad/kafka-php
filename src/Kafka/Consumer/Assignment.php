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

class Assignment
{
    use \Kafka\SingletonTrait;
    // {{{ consts
    // }}}
    // {{{ members
    
    private $memberId = '';

    private $generationId = '';

    private $assignments = array();

    private $topics = array();

    private $offsets = array();

    private $lastOffsets = array();

    private $fetchOffsets = array();

    private $consumerOffsets = array();

    private $commitOffsets = array();

    private $precommitOffsets = array();

    // }}}
    // {{{ functions
    // {{{ public function setMemberId()

    public function setMemberId($memberId)
    {
        $this->memberId = $memberId;
    }

    // }}}
    // {{{ public function getMemberId()

    public function getMemberId()
    {
        return $this->memberId;
    }

    // }}}
    // {{{ public function setGenerationId()

    public function setGenerationId($generationId)
    {
        $this->generationId = $generationId;
    }

    // }}}
    // {{{ public function getGenerationId()

    public function getGenerationId()
    {
        return $this->generationId;
    }

    // }}}
    // {{{ public function getAssignments()

    public function getAssignments()
    {
        return $this->assignments;
    }

    // }}}
    // {{{ public function assign()

    public function assign($result)
    {
        $broker = \Kafka\Broker::getInstance();
        $topics = $broker->getTopics();

        $memberCount = count($result);

        $count = 0;
        $members = array();
        foreach ($topics as $topicName => $partition) {
            foreach ($partition as $partId => $leaderId) {
                $memberNum = $count % $memberCount;
                if (!isset($members[$memberNum])) {
                    $members[$memberNum] = array();
                }
                if (!isset($members[$memberNum][$topicName])) {
                    $members[$memberNum][$topicName] = array();
                }
                $members[$memberNum][$topicName]['topic_name'] = $topicName;
                if (!isset($members[$memberNum][$topicName]['partitions'])) {
                    $members[$memberNum][$topicName]['partitions'] = array();
                }
                $members[$memberNum][$topicName]['partitions'][] = $partId;
                $count++;
            }
        }

        $data = array();
        foreach ($result as $key => $member) {
            $item = array(
                'version' => 0,
                'member_id' => $member['memberId'],
                'assignments' => isset($members[$key]) ? $members[$key] : array()
            );
            $data[] = $item;
        }
        $this->assignments = $data;
    }

    // }}}
    // {{{ public function setTopics()

    public function setTopics($topics)
    {
        $this->topics = $topics;
    }

    // }}}
    // {{{ public function getTopics()

    public function getTopics()
    {
        return $this->topics;
    }

    // }}}
    // {{{ public function setOffsets()

    public function setOffsets($offsets)
    {
        $this->offsets = $offsets;
    }

    // }}}
    // {{{ public function getOffsets()

    public function getOffsets()
    {
        return $this->offsets;
    }

    // }}}
    // {{{ public function setLastOffsets()

    public function setLastOffsets($offsets)
    {
        $this->lastOffsets = $offsets;
    }

    // }}}
    // {{{ public function getOffsets()

    public function getLastOffsets()
    {
        return $this->lastOffsets;
    }

    // }}}
    // {{{ public function setFetchOffsets()

    public function setFetchOffsets($offsets)
    {
        $this->fetchOffsets = $offsets;
    }

    // }}}
    // {{{ public function getFetchOffsets()

    public function getFetchOffsets()
    {
        return $this->fetchOffsets;
    }

    // }}}
    // {{{ public function setConsumerOffsets()

    public function setConsumerOffsets($offsets)
    {
        $this->consumerOffsets = $offsets;
    }

    // }}}
    // {{{ public function getConsumerOffsets()

    public function getConsumerOffsets()
    {
        return $this->consumerOffsets;
    }

    // }}}
    // {{{ public function setConsumerOffset()

    public function setConsumerOffset($topic, $part, $offset)
    {
        $this->consumerOffsets[$topic][$part] = $offset;
    }

    // }}}
    // {{{ public function getConsumerOffset()

    public function getConsumerOffset($topic, $part)
    {
        if (!isset($this->consumerOffsets[$topic][$part])) {
            return false;
        }
        return $this->consumerOffsets[$topic][$part];
    }

    // }}}
    // {{{ public function setCommitOffsets()

    public function setCommitOffsets($offsets)
    {
        $this->commitOffsets = $offsets;
    }

    // }}}
    // {{{ public function getCommitOffsets()

    public function getCommitOffsets()
    {
        return $this->commitOffsets;
    }

    // }}}
    // {{{ public function setCommitOffset()

    public function setCommitOffset($topic, $part, $offset)
    {
        $this->commitOffsets[$topic][$part] = $offset;
    }

    // }}}
    // {{{ public function setPrecommitOffsets()

    public function setPrecommitOffsets($offsets)
    {
        $this->precommitOffsets = $offsets;
    }

    // }}}
    // {{{ public function getPrecommitOffsets()

    public function getPrecommitOffsets()
    {
        return $this->precommitOffsets;
    }

    // }}}
    // {{{ public function setPrecommitOffset()

    public function setPrecommitOffset($topic, $part, $offset)
    {
        $this->precommitOffsets[$topic][$part] = $offset;
    }

    // }}}
    // {{{ public function clearOffset()

    public function clearOffset()
    {
        $this->offsets = array();
        $this->lastOffsets = array();
        $this->fetchOffsets = array();
        $this->consumerOffsets = array();
        $this->commitOffsets = array();
        $this->precommitOffsets = array();
    }

    // }}}
    // }}}
}
