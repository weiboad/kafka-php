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

use Amp\Loop;
use Kafka\Consumer\Process;
use Kafka\Consumer\StopStrategy;

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

class Consumer
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    // {{{ members

    /**
     * @var StopStrategy|null
     */
    private $stopStrategy;

    /**
     * @var Process|null
     */
    private $process;

    // }}}
    // {{{ functions

    public function __construct(?StopStrategy $stopStrategy = null)
    {
        $this->stopStrategy = $stopStrategy;
    }

    // {{{ public function start()

    /**
     * start consumer
     *
     * @access public
     *
     * @param callable|null $consumer
     *
     * @return void
     */
    public function start(?callable $consumer = null): void
    {
        if ($this->process !== null) {
            $this->error('Consumer is already being executed');
            return;
        }

        $this->setupStopStrategy();

        $this->process = $this->createProcess($consumer);
        $this->process->start();

        Loop::run();
    }

    /**
     * FIXME: remove it when we implement dependency injection
     *
     * This is a very bad practice, but if we don't create this method
     * this class will never be testable...
     *
     * @codeCoverageIgnore
     */
    protected function createProcess(?callable $consumer): Process
    {
        $process = new Process($consumer);

        if ($this->logger) {
            $process->setLogger($this->logger);
        }

        return $process;
    }

    private function setupStopStrategy(): void
    {
        if ($this->stopStrategy === null) {
            return;
        }

        $this->stopStrategy->setup($this);
    }

    public function stop(): void
    {
        if ($this->process === null) {
            $this->error('Consumer is not running');
            return;
        }

        $this->process->stop();
        $this->process = null;

        Loop::stop();
    }

    // }}}
    // }}}
}
