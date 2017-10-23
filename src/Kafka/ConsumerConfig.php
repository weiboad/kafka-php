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

class ConsumerConfig extends Config
{
    use SingletonTrait;
    // {{{ consts
    // }}}
    // {{{ members

    protected static $defaults = array(
        'groupId' => '',
        'sessionTimeout' => 30000,
        'rebalanceTimeout' => 30000,
        'topics' => array(),
        'offsetReset' => 'latest', // earliest
        'maxBytes' => 65536, // 64kb
        'maxWaitTime' => 100,
    );

    // }}}
    // {{{ functions
    // {{{ public function getGroupId()

    public function getGroupId()
    {
        $groupId = trim($this->ietGroupId());

        if ($groupId == false || $groupId == '') {
            throw new \Kafka\Exception\Config('Get group id value is invalid, must set it not empty string');
        }

        return $groupId;
    }

    // }}}
    // {{{ public function setGroupId()

    public function setGroupId($groupId)
    {
        $groupId = trim($groupId);
        if ($groupId == false || $groupId == '') {
            throw new \Kafka\Exception\Config('Set group id value is invalid, must set it not empty string');
        }
        static::$options['groupId'] = $groupId;
    }

    // }}}
    // {{{ public function setSessionTimeout()

    public function setSessionTimeout($sessionTimeout)
    {
        if (!is_numeric($sessionTimeout) || $sessionTimeout < 1 || $sessionTimeout > 3600000) {
            throw new \Kafka\Exception\Config('Set session timeout value is invalid, must set it 1 .. 3600000');
        }
        static::$options['sessionTimeout'] = $sessionTimeout;
    }

    // }}}
    // {{{ public function setRebalanceTimeout()

    public function setRebalanceTimeout($rebalanceTimeout)
    {
        if (!is_numeric($rebalanceTimeout) || $rebalanceTimeout < 1 || $rebalanceTimeout > 3600000) {
            throw new \Kafka\Exception\Config('Set rebalance timeout value is invalid, must set it 1 .. 3600000');
        }
        static::$options['rebalanceTimeout'] = $rebalanceTimeout;
    }

    // }}}
    // {{{ public function setOffsetReset()

    public function setOffsetReset($offsetReset)
    {
        if (!in_array($offsetReset, array('latest', 'earliest'))) {
            throw new \Kafka\Exception\Config('Set offset reset value is invalid, must set it `latest` or `earliest`');
        }
        static::$options['offsetReset'] = $offsetReset;
    }

    // }}}
    // {{{ public function getTopics()

    public function getTopics()
    {
        $topics = $this->ietTopics();

        if (empty($topics)) {
            throw new \Kafka\Exception\Config('Get consumer topics value is invalid, must set it not empty');
        }

        return $topics;
    }

    // }}}
    // {{{ public function setTopics()

    public function setTopics($topics)
    {
        if (!is_array($topics) || empty($topics)) {
            throw new \Kafka\Exception\Config('Set consumer topics value is invalid, must set it not empty array');
        }
        static::$options['topics'] = $topics;
    }

    // }}}
    // }}}

    protected $runtime_options = [
        'consume_mode' => self::CONSUME_AFTER_COMMIT_OFFSET
    ];
    const CONSUME_AFTER_COMMIT_OFFSET = 1;
    const CONSUME_BEFORE_COMMIT_OFFSET = 2;

    public function setConsumeMode($mode)
    {
        $this->runtime_options['consume_mode'] = $mode;
    }

    public function getConsumeMode()
    {
        return $this->runtime_options['consume_mode'];
    }
}
