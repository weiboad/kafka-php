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

class Producer
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    // {{{ consts
    // }}}
    // {{{ members
    
    private $process = null;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    /**
     * __construct
     *
     * @access public
     * @param $hostList
     * @param null $timeout
     */
    public function __construct(\Closure $producer = null)
    {
        if (is_null($producer)) {
            $this->process = new \Kafka\Producer\SyncProcess();
        } else {
            $this->process = new \Kafka\Producer\Process($producer);
        }
    }

    // }}}
    // {{{ public function send()

    /**
     * start producer
     *
     * @access public
     * @data is data is boolean that is async process, thus it is sync process
     * @return void
     */
    public function send($data = true)
    {
        if ($this->logger) {
            $this->process->setLogger($this->logger);
        }
        if (is_bool($data)) {
            $this->process->start();
            if ($data) {
                \Amp\run();
            }
        } else {
            return $this->process->send($data);
        }
    }

    // }}}
    // {{{ public function syncMeta()

    /**
     * syncMeta producer
     *
     * @access public
     * @return void
     */
    public function syncMeta()
    {
        return $this->process->syncMeta();
    }

    // }}}
    // {{{ public function success()

    /**
     * producer success
     *
     * @access public
     * @return void
     */
    public function success(\Closure $success = null)
    {
        $this->process->setSuccess($success);
    }

    // }}}
    // {{{ public function error()

    /**
     * producer error
     *
     * @access public
     * @return void
     */
    public function error(\Closure $error = null)
    {
        $this->process->setError($error);
    }

    // }}}
    // }}}
}
