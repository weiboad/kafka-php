<?php
namespace KafkaTest\Base\StreamStub;

class Stream
{
    public function open($path, $mode, $options)
    {
        return true;
    }

    public function context($context)
    {
        return true;
    }

    public function eof()
    {
        return false;
    }

    public function read($length)
    {
        return false;
    }

    public function write($data)
    {
        return strlen($data);
    }

    public function metadata($path, $option, $var)
    {
        return false;
    }

    public function option($option, $args1, $args2)
    {
        return true;
    }
}
