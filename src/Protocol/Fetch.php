<?php
declare(strict_types=1);

namespace Kafka\Protocol;

use Kafka\Exception;
use Kafka\Exception\NotSupported;
use Kafka\Exception\Protocol as ProtocolException;

class Fetch extends Protocol
{
    /**
     * @param mixed[] $payloads
     *
     * @throws NotSupported
     * @throws ProtocolException
     */
    public function encode(array $payloads = []): string
    {
        if (! isset($payloads['data'])) {
            throw new ProtocolException('given fetch kafka data invalid. `data` is undefined.');
        }

        if (! isset($payloads['replica_id'])) {
            $payloads['replica_id'] = -1;
        }

        if (! isset($payloads['max_wait_time'])) {
            $payloads['max_wait_time'] = 100; // default timeout 100ms
        }

        if (! isset($payloads['min_bytes'])) {
            $payloads['min_bytes'] = 64 * 1024; // 64k
        }

        $header = $this->requestHeader('kafka-php', self::FETCH_REQUEST, self::FETCH_REQUEST);
        $data   = self::pack(self::BIT_B32, (string) $payloads['replica_id']);
        $data  .= self::pack(self::BIT_B32, (string) $payloads['max_wait_time']);
        $data  .= self::pack(self::BIT_B32, (string) $payloads['min_bytes']);
        $data  .= self::encodeArray($payloads['data'], [$this, 'encodeFetchTopic']);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    /**
     * @return mixed[]
     */
    public function decode(string $data): array
    {
        $offset       = 0;
        $version      = $this->getApiVersion(self::FETCH_REQUEST);
        $throttleTime = 0;

        if ($version !== self::API_VERSION0) {
            $throttleTime = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $offset      += 4;
        }

        $topics  = $this->decodeArray(substr($data, $offset), [$this, 'fetchTopic']);
        $offset += $topics['length'];

        return [
            'throttleTime' => $throttleTime,
            'topics'       => $topics['data'],
        ];
    }

    /**
     * @return mixed[]
     */
    protected function fetchTopic(string $data): array
    {
        $offset    = 0;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset   += $topicInfo['length'];

        $partitions = $this->decodeArray(substr($data, $offset), [$this, 'fetchPartition']);
        $offset    += $partitions['length'];

        return [
            'length' => $offset,
            'data'   => [
                'topicName'  => $topicInfo['data'],
                'partitions' => $partitions['data'],
            ],
        ];
    }

    /**
     * @return mixed[]
     *
     * @throws ProtocolException
     */
    protected function fetchPartition(string $data): array
    {
        $offset              = 0;
        $partitionId         = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset             += 4;
        $errorCode           = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset             += 2;
        $highwaterMarkOffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
        $offset             += 8;

        $messageSetSize = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset        += 4;

        $messages = [];

        if ($messageSetSize > 0 && $offset < \strlen($data)) {
            $messages = $this->decodeMessageSetArray(substr($data, $offset, $messageSetSize), $messageSetSize);

            $offset += $messages['length'];
        }

        return [
            'length' => $offset,
            'data'   => [
                'partition'           => $partitionId,
                'errorCode'           => $errorCode,
                'highwaterMarkOffset' => $highwaterMarkOffset,
                'messageSetSize'      => $messageSetSize,
                'messages'            => $messages['data'] ?? [],
            ],
        ];
    }

    /**
     * @return mixed[]
     *
     * @throws ProtocolException
     */
    protected function decodeMessageSetArray(string $data, int $messageSetSize): array
    {
        $offset = 0;
        $result = [];

        while ($offset < \strlen($data)) {
            $value = \substr($data, $offset);
            $ret   = $this->decodeMessageSet($value);

            if ($ret === null) {
                break;
            }

            if (! isset($ret['length'], $ret['data'])) {
                throw new ProtocolException('Decode array failed, given function return format is invalid');
            }

            if ((int) $ret['length'] === 0) {
                continue;
            }

            $offset += $ret['length'];

            if (($ret['data']['message']['attr'] & Produce::COMPRESSION_CODEC_MASK) === Produce::COMPRESSION_NONE) {
                $result[] = $ret['data'];
                continue;
            }

            $innerMessages = $this->decodeMessageSetArray($ret['data']['message']['value'], $ret['length']);
            $result        = \array_merge($result, $innerMessages['data']);
        }

        if ($offset < $messageSetSize) {
            $offset = $messageSetSize;
        }

        return ['length' => $offset, 'data' => $result];
    }

    /**
     * decode message set
     * N.B., MessageSets are not preceded by an int32 like other array elements
     * in the protocol.
     *
     * @return mixed[]|null
     */
    protected function decodeMessageSet(string $data): ?array
    {
        if (strlen($data) <= 12) {
            return null;
        }

        $offset      = 0;
        $roffset     = self::unpack(self::BIT_B64, substr($data, $offset, 8));
        $offset     += 8;
        $messageSize = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset     += 4;
        $ret         = $this->decodeMessage(substr($data, $offset), $messageSize);

        if ($ret === null) {
            return null;
        }

        $offset += $ret['length'];

        return [
            'length' => $offset,
            'data'   => [
                'offset'  => $roffset,
                'size'    => $messageSize,
                'message' => $ret['data'],
            ],
        ];
    }

    /**
     * decode message
     * N.B., MessageSets are not preceded by an int32 like other array elements
     * in the protocol.
     *
     * @return mixed[]|null
     */
    protected function decodeMessage(string $data, int $messageSize): ?array
    {
        if ($messageSize === 0 || \strlen($data) < $messageSize) {
            return null;
        }

        $offset  = 0;
        $crc     = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;

        $magic = self::unpack(self::BIT_B8, substr($data, $offset, 1));
        ++$offset;

        $attr = self::unpack(self::BIT_B8, substr($data, $offset, 1));
        ++$offset;

        $timestamp  = 0;
        $backOffset = $offset;

        try { // try unpack message format v1, falling back to v0 if it fails
            if ($magic >= self::MESSAGE_MAGIC_VERSION1) {
                $timestamp = self::unpack(self::BIT_B64, substr($data, $offset, 8));
                $offset   += 8;
            }

            $keyRet  = $this->decodeString(substr($data, $offset), self::BIT_B32);
            $offset += $keyRet['length'];

            $valueRet = $this->decodeString((string) substr($data, $offset), self::BIT_B32, $attr & Produce::COMPRESSION_CODEC_MASK);
            $offset  += $valueRet['length'];

            if ($offset !== $messageSize) {
                throw new Exception(
                    'pack message fail, message len:' . $messageSize . ' , data unpack offset :' . $offset
                );
            }
        } catch (Exception $e) { // try unpack message format v0
            $offset    = $backOffset;
            $timestamp = 0;
            $keyRet    = $this->decodeString(substr($data, $offset), self::BIT_B32);
            $offset   += $keyRet['length'];

            $valueRet = $this->decodeString(substr($data, $offset), self::BIT_B32);
            $offset  += $valueRet['length'];
        }

        return [
            'length' => $offset,
            'data'   => [
                'crc'       => $crc,
                'magic'     => $magic,
                'attr'      => $attr,
                'timestamp' => $timestamp,
                'key'       => $keyRet['data'],
                'value'     => $valueRet['data'],
            ],
        ];
    }

    /**
     * @param mixed[] $values
     *
     * @throws ProtocolException
     */
    protected function encodeFetchPartition(array $values): string
    {
        if (! isset($values['partition_id'])) {
            throw new ProtocolException('given fetch data invalid. `partition_id` is undefined.');
        }

        if (! isset($values['offset'])) {
            $values['offset'] = 0;
        }

        if (! isset($values['max_bytes'])) {
            $values['max_bytes'] = 2 * 1024 * 1024;
        }

        $data  = self::pack(self::BIT_B32, (string) $values['partition_id']);
        $data .= self::pack(self::BIT_B64, (string) $values['offset']);
        $data .= self::pack(self::BIT_B32, (string) $values['max_bytes']);

        return $data;
    }

    /**
     * @param mixed[] $values
     *
     * @throws NotSupported
     * @throws ProtocolException
     */
    protected function encodeFetchTopic(array $values): string
    {
        if (! isset($values['topic_name'])) {
            throw new ProtocolException('given fetch data invalid. `topic_name` is undefined.');
        }

        if (! isset($values['partitions']) || empty($values['partitions'])) {
            throw new ProtocolException('given fetch data invalid. `partitions` is undefined.');
        }

        $topic      = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], [$this, 'encodeFetchPartition']);

        return $topic . $partitions;
    }
}
