<?php
declare(strict_types=1);

namespace KafkaTest\Base\StreamStub;

use function str_repeat;
use function strlen;

class Stream
{
    public function open(string $path, string $mode, int $options): bool
    {
        return true;
    }

    /**
     * @param mixed[] $context
     */
    public function context(array $context): bool
    {
        return true;
    }

    public function eof(): bool
    {
        return false;
    }

    public function read(int $length): string
    {
        return str_repeat('x', $length);
    }

    public function write(string $data): ?int
    {
        return strlen($data);
    }

    /**
     * @param mixed $value
     */
    public function metadata(string $path, string $option, $value): bool
    {
        return false;
    }

    /**
     * @param mixed ...$options
     */
    public function option(...$options): bool
    {
        return true;
    }
}
