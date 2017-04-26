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

class Broker extends \Kafka\Singleton
{
    // {{{ consts
    // }}}
    // {{{ members

    private $groupBrokerId = null;

    private $topics = array();

    // }}}
    // {{{ functions
    // {{{ public function setGroupBrokerId()

    public function setGroupBrokerId($brokerId)
    {
        $this->groupBrokerId = $brokerId;
    }

    // }}}
    // {{{ public function getGroupBrokerId()

    public function getGroupBrokerId()
    {
        return $this->groupBrokerId;
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
    // }}}
}
