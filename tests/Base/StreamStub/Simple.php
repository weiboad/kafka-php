<?php
namespace KafkaTest\Base\StreamStub;

class Simple
{
    public $context;

    protected static $mock;

    public function stream_open($path, $mode, $options, &$opened_path)
    {
        if (self::$mock !== null) {
            self::$mock->context(stream_context_get_options($this->context));
            return self::$mock->open($path, $mode, $options);
        }

        return true;
    }

    public function stream_eof()
    {
        if (self::$mock !== null) {
            return self::$mock->eof();
        }
        return false;
    }

    public function stream_read($len)
    {
        if (self::$mock !== null) {
            return self::$mock->read($len);
        }
        return true;
    }

    public function stream_write($data)
    {
        if (self::$mock !== null) {
            return self::$mock->write($data);
        }
        return true;
    }

    public function stream_metadata($path, $option, $var)
    {
        if (self::$mock !== null) {
            return self::$mock->metadata($path, $option, $var);
        }
        return true;
    }

    public function stream_set_option($option, $arg1, $arg2)
    {
        if (self::$mock !== null) {
            return self::$mock->option($option, $arg1, $arg2);
        }
        return true;
    }

    public static function setMock($mock)
    {
        self::$mock = $mock;
    }
}
