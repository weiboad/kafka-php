<?php
declare(strict_types=1);

namespace Kafka\Protocol;

use Kafka\Exception\NotSupported;
use Kafka\Exception\Protocol as ProtocolException;
use Lcobucci\Clock\Clock;
use Lcobucci\Clock\SystemClock;
use function crc32;
use function is_array;
use function substr;

class Produce extends Protocol
{
    /**
     * Specifies the mask for the compression code. 3 bits to hold the compression codec.
     * 0 is reserved to indicate no compression
     */
    public const COMPRESSION_CODEC_MASK = 0x07;

    /**
     * Specify the mask of timestamp type: 0 for CreateTime, 1 for LogAppendTime.
     */
    private const TIMESTAMP_TYPE_MASK = 0x08;

    private const TIMESTAMP_NONE            = -1;
    private const TIMESTAMP_CREATE_TIME     = 0;
    private const TIMESTAMP_LOG_APPEND_TIME = 1;

    /**
     * @var Clock
     */
    private $clock;

    public function __construct(string $version = self::DEFAULT_BROKER_VERION, ?Clock $clock = null)
    {
        parent::__construct($version);

        $this->clock = $clock ?: new SystemClock();
    }

    /**
     * @param mixed[] $payloads
     *
     * @throws NotSupported
     * @throws ProtocolException
     */
    public function encode(array $payloads = []): string
    {
        if (! isset($payloads['data'])) {
            throw new ProtocolException('given procude data invalid. `data` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', 0, self::PRODUCE_REQUEST);
        $data   = self::pack(self::BIT_B16, (string) ($payloads['required_ack'] ?? 0));
        $data  .= self::pack(self::BIT_B32, (string) ($payloads['timeout'] ?? 100));
        $data  .= self::encodeArray(
            $payloads['data'],
            [$this, 'encodeProduceTopic'],
            $payloads['compression'] ?? self::COMPRESSION_NONE
        );

        return self::encodeString($header . $data, self::PACK_INT32);
    }

    /**
     * @return mixed[]
     *
     * @throws ProtocolException
     */
    public function decode(string $data): array
    {
        $offset       = 0;
        $version      = $this->getApiVersion(self::PRODUCE_REQUEST);
        $ret          = $this->decodeArray(substr($data, $offset), [$this, 'produceTopicPair'], $version);
        $offset      += $ret['length'];
        $throttleTime = 0;

        if ($version === self::API_VERSION2) {
            $throttleTime = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        }

        return ['throttleTime' => $throttleTime, 'data' => $ret['data']];
    }

    /**
     * encode message set
     * N.B., MessageSets are not preceded by an int32 like other array elements
     * in the protocol.
     *
     * @param string[]|string[][] $messages
     *
     * @throws NotSupported
     */
    protected function encodeMessageSet(array $messages, int $compression = self::COMPRESSION_NONE): string
    {
        $data = '';
        $next = 0;

        foreach ($messages as $message) {
            $encodedMessage = $this->encodeMessage($message);

            $data .= self::pack(self::BIT_B64, (string) $next)
                   . self::encodeString($encodedMessage, self::PACK_INT32);

            ++$next;
        }

        if ($compression === self::COMPRESSION_NONE) {
            return $data;
        }

        return self::pack(self::BIT_B64, '0')
             . self::encodeString($this->encodeMessage($data, $compression), self::PACK_INT32);
    }

    /**
     * @param string[]|string $message
     *
     * @throws NotSupported
     */
    protected function encodeMessage($message, int $compression = self::COMPRESSION_NONE): string
    {
        $magic      = $this->computeMagicByte();
        $attributes = $this->computeAttributes($magic, $compression, $this->computeTimestampType($magic));

        $data  = self::pack(self::BIT_B8, (string) $magic);
        $data .= self::pack(self::BIT_B8, (string) $attributes);

        if ($magic >= self::MESSAGE_MAGIC_VERSION1) {
            $data .= self::pack(self::BIT_B64, $this->clock->now()->format('Uv'));
        }

        $key = '';

        if (is_array($message)) {
            $key     = $message['key'];
            $message = $message['value'];
        }

        // message key
        $data .= self::encodeString($key, self::PACK_INT32);

        // message value
        $data .= self::encodeString($message, self::PACK_INT32, $compression);

        $crc = (string) crc32($data);

        // int32 -- crc code  string data
        $message = self::pack(self::BIT_B32, $crc) . $data;

        return $message;
    }

    private function computeMagicByte(): int
    {
        if ($this->getApiVersion(self::PRODUCE_REQUEST) === self::API_VERSION2) {
            return self::MESSAGE_MAGIC_VERSION1;
        }

        return self::MESSAGE_MAGIC_VERSION0;
    }

    public function computeTimestampType(int $magic): int
    {
        if ($magic === self::MESSAGE_MAGIC_VERSION0) {
            return self::TIMESTAMP_NONE;
        }

        return self::TIMESTAMP_CREATE_TIME;
    }

    private function computeAttributes(int $magic, int $compression, int $timestampType): int
    {
        $attributes = 0;

        if ($compression !== self::COMPRESSION_NONE) {
            $attributes |= self::COMPRESSION_CODEC_MASK & $compression;
        }

        if ($magic === self::MESSAGE_MAGIC_VERSION0) {
            return $attributes;
        }

        if ($timestampType === self::TIMESTAMP_LOG_APPEND_TIME) {
            $attributes |= self::TIMESTAMP_TYPE_MASK;
        }

        return $attributes;
    }

    /**
     * encode signal part
     *
     * @param mixed[] $values
     *
     * @throws NotSupported
     * @throws ProtocolException
     */
    protected function encodeProducePartition(array $values, int $compression): string
    {
        if (! isset($values['partition_id'])) {
            throw new ProtocolException('given produce data invalid. `partition_id` is undefined.');
        }

        if (! isset($values['messages']) || empty($values['messages'])) {
            throw new ProtocolException('given produce data invalid. `messages` is undefined.');
        }

        $data  = self::pack(self::BIT_B32, (string) $values['partition_id']);
        $data .= self::encodeString(
            $this->encodeMessageSet((array) $values['messages'], $compression),
            self::PACK_INT32
        );

        return $data;
    }

    /**
     * encode signal topic
     *
     * @param mixed[] $values
     *
     * @throws NotSupported
     * @throws ProtocolException
     */
    protected function encodeProduceTopic(array $values, int $compression): string
    {
        if (! isset($values['topic_name'])) {
            throw new ProtocolException('given produce data invalid. `topic_name` is undefined.');
        }

        if (! isset($values['partitions']) || empty($values['partitions'])) {
            throw new ProtocolException('given produce data invalid. `partitions` is undefined.');
        }

        $topic      = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], [$this, 'encodeProducePartition'], $compression);

        return $topic . $partitions;
    }

    /**
     * decode produce topic pair response
     *
     * @return mixed[]
     *
     * @throws ProtocolException
     */
    protected function produceTopicPair(string $data, int $version): array
    {
        $offset    = 0;
        $topicInfo = $this->decodeString($data, self::BIT_B16);
        $offset   += $topicInfo['length'];
        $ret       = $this->decodeArray(substr($data, $offset), [$this, 'producePartitionPair'], $version);
        $offset   += $ret['length'];

        return [
            'length' => $offset,
            'data'   => [
                'topicName'  => $topicInfo['data'],
                'partitions' => $ret['data'],
            ],
        ];
    }

    /**
     * decode produce partition pair response
     *
     * @return mixed[]
     *
     * @throws ProtocolException
     */
    protected function producePartitionPair(string $data, int $version): array
    {
        $offset          = 0;
        $partitionId     = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset         += 4;
        $errorCode       = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset         += 2;
        $partitionOffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
        $offset         += 8;
        $timestamp       = 0;

        if ($version === self::API_VERSION2) {
            $timestamp = self::unpack(self::BIT_B64, substr($data, $offset, 8));
            $offset   += 8;
        }

        return [
            'length' => $offset,
            'data'   => [
                'partition' => $partitionId,
                'errorCode' => $errorCode,
                'offset'    => $offset,
                'timestamp' => $timestamp,
            ],
        ];
    }
}
