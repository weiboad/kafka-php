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

class Broker
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    // {{{ consts
    // }}}
    // {{{ members
    
    private static $instance = null;

    private $groupBrokerId = null;

    private $topics = array();

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
    // {{{ public function setGroupBrokerId()

    public function setGroupBrokerId($brokerId) {
        $this->groupBrokerId = $brokerId;
    }

    // }}}
    // {{{ public function getGroupBrokerId()

    public function getGroupBrokerId() {
        return $this->groupBrokerId;
    }

    // }}}
    // {{{ public function setTopics()

    public function setTopics($topics) {
        $this->topics = $topics;
    }

    // }}}
    // {{{ public function getTopics()

    public function getTopics() {
        return $this->topics;
    }

    // }}}
    // }}}
}
