<?php

namespace Kafka\Protocol;

use Lcobucci\Clock\Clock;
use Lcobucci\Clock\SystemClock;

class Produce extends Protocol
{
    /**
     * Specifies the mask for the compression code. 3 bits to hold the compression codec.
     * 0 is reserved to indicate no compression
     */
    private const COMPRESSION_CODEC_MASK = 0x07;

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

    public function encode(array $payloads = []): string
    {
        if (! isset($payloads['data'])) {
            throw new \Kafka\Exception\Protocol('given procude data invalid. `data` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', 0, self::PRODUCE_REQUEST);
        $data   = self::pack(self::BIT_B16, $payloads['required_ack'] ?? 0);
        $data  .= self::pack(self::BIT_B32, $payloads['timeout'] ?? 100);
        $data  .= self::encodeArray(
            $payloads['data'],
            [$this, 'encodeProduceTopic'],
            self::COMPRESSION_NONE
        );
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

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
     */
    protected function encodeMessageSet(array $messages, int $compression = self::COMPRESSION_NONE): string
    {
        $data = '';
        $next = 0;

        foreach ($messages as $message) {
            $tmpMessage = $this->encodeMessage($message, $compression);

            // int64 -- message offset     Message
            // This is the offset used in kafka as the log sequence number. When the producer is sending non compressed messages,
            // it can set the offsets to anything. When the producer is sending compressed messages, to avoid server side recompression,
            // each compressed message should have offset starting from 0 and increasing by one for each inner message in the compressed
            // message. (see more details about compressed messages in Kafka below)
            $data .= self::pack(self::BIT_B64, $next) . self::encodeString($tmpMessage, self::PACK_INT32);
            $next++;
        }

        return $data;
    }

    /**
     * @param array|string $message
     */
    protected function encodeMessage($message, int $compression = self::COMPRESSION_NONE): string
    {
        $magic      = $this->computeMagicBit();
        $attributes = $this->computeAttributes($magic, $compression, $this->computeTimestampType($magic));

        $data  = self::pack(self::BIT_B8, $magic);
        $data .= self::pack(self::BIT_B8, $attributes);

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

        $crc = crc32($data);

        // int32 -- crc code  string data
        $message = self::pack(self::BIT_B32, $crc) . $data;

        return $message;
    }

    private function computeMagicBit(): int
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
     */
    protected function encodeProducePartition(array $values, int $compression): string
    {
        if (! isset($values['partition_id'])) {
            throw new \Kafka\Exception\Protocol('given produce data invalid. `partition_id` is undefined.');
        }

        if (! isset($values['messages']) || empty($values['messages'])) {
            throw new \Kafka\Exception\Protocol('given produce data invalid. `messages` is undefined.');
        }

        $data  = self::pack(self::BIT_B32, $values['partition_id']);
        $data .= self::encodeString(
            $this->encodeMessageSet((array) $values['messages'], $compression),
            self::PACK_INT32
        );

        return $data;
    }

    /**
     * encode signal topic
     */
    protected function encodeProduceTopic(array $values, int $compression): string
    {
        if (! isset($values['topic_name'])) {
            throw new \Kafka\Exception\Protocol('given produce data invalid. `topic_name` is undefined.');
        }

        if (! isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception\Protocol('given produce data invalid. `partitions` is undefined.');
        }

        $topic      = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], [$this, 'encodeProducePartition'], $compression);

        return $topic . $partitions;
    }

    /**
     * decode produce topic pair response
     *
     * @access protected
     * @return array
     */
    protected function produceTopicPair($data, $version)
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
     * @access protected
     * @return array
     */
    protected function producePartitionPair($data, $version)
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
