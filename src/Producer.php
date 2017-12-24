<?php
namespace Kafka;

use Kafka\Producer\Process;
use Kafka\Producer\SyncProcess;

class Producer
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    /**
     * @var Process|SyncProcess
     */
    private $process;

    public function __construct(?callable $producer = null)
    {
        $this->process = $producer === null ? new SyncProcess() : new Process($producer);
    }

    /**
     * @param array|bool $data
     */
    public function send($data = true): ?array
    {
        if ($this->logger) {
            $this->process->setLogger($this->logger);
        }

        if (! \is_bool($data)) {
            return $this->process->send($data);
        }

        $this->process->start();

        if ($data) {
            \Amp\Loop::run();
        }

        return null;
    }

    public function syncMeta(): void
    {
        $this->process->syncMeta();
    }

    public function success(callable $success = null): void
    {
        $this->process->setSuccess($success);
    }

    public function error(callable $error = null): void
    {
        $this->process->setError($error);
    }
}
