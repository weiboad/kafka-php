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
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    // {{{ consts
    // }}}
    // {{{ members
    
    private static $instance = null;

    private $memberId = '';
    private $generationId = '';

    private $assignments = array();

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
    // {{{ public function setMemberId()

    public function setMemberId($memberId) {
        $this->memberId = $memberId;
    }

    // }}}
    // {{{ public function getMemberId()

    public function getMemberId() {
        return $this->memberId;
    }

    // }}}
    // {{{ public function setGenerationId()

    public function setGenerationId($generationId) {
        $this->generationId = $generationId;
    }

    // }}}
    // {{{ public function getGenerationId()

    public function getGenerationId() {
        return $this->generationId;
    }

    // }}}
    // {{{ public function getAssignments()

    public function getAssignments() {
        return $this->assignments;
    }

    // }}}
    // {{{ public function assign()

    public function assign($result) {
        $broker = \Kafka\Consumer\Broker::getInstance();
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
                'assignments' => $members[$key]
            );
            $data[] = $item;
        }
        $this->assignments = $data;
    }

    // }}}
    // }}}
}
