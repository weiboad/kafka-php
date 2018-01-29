<?php
declare(strict_types=1);

namespace KafkaTest\Base\StreamStub;

use PHPUnit\Framework\MockObject\MockObject;
use function stream_context_get_options;

class Simple
{
    /**
     * @var resource
     */
    public $context;

    /**
     * @var Stream|MockObject
     */
    protected static $mock;

    public function stream_open(string $path, string $mode, int $options): bool
    {
        if (self::$mock !== null) {
            self::$mock->context(stream_context_get_options($this->context));

            return self::$mock->open($path, $mode, $options);
        }

        return true;
    }

    public function stream_eof(): bool
    {
        if (self::$mock !== null) {
            return self::$mock->eof() ?? false;
        }

        return false;
    }

    public function stream_read(int $length): string
    {
        if (self::$mock !== null) {
            return self::$mock->read($length) ?? '';
        }

        return '';
    }

    public function stream_write(string $data): int
    {
        if (self::$mock !== null) {
            return self::$mock->write($data) ?? 0;
        }

        return 1;
    }

    /**
     * @param mixed $value
     */
    public function stream_metadata(string $path, string $option, $value): bool
    {
        if (self::$mock !== null) {
            return self::$mock->metadata($path, $option, $value);
        }

        return true;
    }

    /**
     * @param mixed ...$options
     */
    public function stream_set_option(...$options): bool
    {
        if (self::$mock !== null) {
            return self::$mock->option(...$options);
        }

        return true;
    }

    /**
     * @param Stream|MockObject $mock
     */
    public static function setMock(Stream $mock): void
    {
        self::$mock = $mock;
    }
}
