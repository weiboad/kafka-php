<?php
namespace Kafka;

use Kafka\Protocol\Produce;

/**
 * @method int getRequestTimeout()
 * @method int getProduceInterval()
 * @method int getTimeout()
 * @method int getRequiredAck()
 * @method bool getIsAsyn()
 * @method int getCompression()
 */
class ProducerConfig extends Config
{
    use SingletonTrait;

    private const COMPRESSION_OPTIONS = [
        Produce::COMPRESSION_NONE,
        Produce::COMPRESSION_GZIP,
        Produce::COMPRESSION_SNAPPY,
    ];

    protected static $defaults = [
        'requiredAck'     => 1,
        'timeout'         => 5000,
        'isAsyn'          => false,
        'requestTimeout'  => 6000,
        'produceInterval' => 100,
        'compression'     => Protocol\Protocol::COMPRESSION_NONE,
    ];

    public function setRequestTimeout($requestTimeout): void
    {
        if (! is_numeric($requestTimeout) || $requestTimeout < 1 || $requestTimeout > 900000) {
            throw new Exception\Config('Set request timeout value is invalid, must set it 1 .. 900000');
        }

        static::$options['requestTimeout'] = $requestTimeout;
    }

    public function setProduceInterval($produceInterval): void
    {
        if (! is_numeric($produceInterval) || $produceInterval < 1 || $produceInterval > 900000) {
            throw new Exception\Config('Set produce interval timeout value is invalid, must set it 1 .. 900000');
        }

        static::$options['produceInterval'] = $produceInterval;
    }

    public function setTimeout($timeout): void
    {
        if (! is_numeric($timeout) || $timeout < 1 || $timeout > 900000) {
            throw new Exception\Config('Set timeout value is invalid, must set it 1 .. 900000');
        }

        static::$options['timeout'] = $timeout;
    }

    public function setRequiredAck($requiredAck): void
    {
        if (! is_numeric($requiredAck) || $requiredAck < -1 || $requiredAck > 1000) {
            throw new Exception\Config('Set required ack value is invalid, must set it -1 .. 1000');
        }

        static::$options['requiredAck'] = $requiredAck;
    }

    public function setIsAsyn($asyn): void
    {
        if (! is_bool($asyn)) {
            throw new Exception\Config('Set isAsyn value is invalid, must set it bool value');
        }

        static::$options['isAsyn'] = $asyn;
    }

    public function setCompression(int $compression): void
    {
        if (! \in_array($compression, self::COMPRESSION_OPTIONS, true)) {
            throw new Exception\Config('Compression must be one the Kafka\Protocol\Produce::COMPRESSION_* constants');
        }

        static::$options['compression'] = $compression;
    }
}
