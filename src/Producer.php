<?php
namespace Kafka;

use Amp\Loop;
use Kafka\Producer\Process;
use Kafka\Producer\SyncProcess;
use Psr\Log\LoggerAwareTrait;

class Producer
{
    use LoggerAwareTrait;
    use LoggerTrait;

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

        if (\is_array($data)) {
            return $this->sendSynchronously($data);
        }

        $this->sendAsynchronously($data);

        return null;
    }

    private function sendSynchronously(array $data): array
    {
        if (! $this->process instanceof SyncProcess) {
            throw new Exception('An asynchronous process is not able to send messages synchronously');
        }

        return $this->process->send($data);
    }

    private function sendAsynchronously(bool $startLoop): void
    {
        if ($this->process instanceof SyncProcess) {
            throw new Exception('A synchronous process is not able to send messages asynchronously');
        }

        $this->process->start();

        if ($startLoop) {
            Loop::run();
        }
    }

    public function syncMeta(): void
    {
        $this->process->syncMeta();
    }

    public function success(callable $success): void
    {
        if ($this->process instanceof SyncProcess) {
            throw new Exception('Success callback can only be configured for asynchronous process');
        }

        $this->process->setSuccess($success);
    }

    public function error(callable $error): void
    {
        if ($this->process instanceof SyncProcess) {
            throw new Exception('Error callback can only be configured for asynchronous process');
        }

        $this->process->setError($error);
    }
}
