<?php

namespace Kafka\Protocol;

use Kafka\Exception\NotSupported;
use Kafka\Exception\Protocol as ProtocolException;

class FetchOffset extends Protocol
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
            throw new ProtocolException('given fetch offset data invalid. `data` is undefined.');
        }

        if (! isset($payloads['group_id'])) {
            throw new ProtocolException('given fetch offset data invalid. `group_id` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', self::OFFSET_FETCH_REQUEST, self::OFFSET_FETCH_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data  .= self::encodeArray($payloads['data'], [$this, 'encodeOffsetTopic']);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    /**
     * @return mixed[]
     */
    public function decode(string $data): array
    {
        $offset  = 0;
        $topics  = $this->decodeArray(substr($data, $offset), [$this, 'offsetTopic']);
        $offset += $topics['length'];

        return $topics['data'];
    }

    protected function encodeOffsetPartition(int $values): string
    {
        return self::pack(self::BIT_B32, (string) $values);
    }

    /**
     * @param mixed[] $values
     *
     * @throws NotSupported
     * @throws ProtocolException
     */
    protected function encodeOffsetTopic(array $values): string
    {
        if (! isset($values['topic_name'])) {
            throw new ProtocolException('given fetch offset data invalid. `topic_name` is undefined.');
        }

        if (! isset($values['partitions']) || empty($values['partitions'])) {
            throw new ProtocolException('given fetch offset data invalid. `partitions` is undefined.');
        }

        $topic      = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], [$this, 'encodeOffsetPartition']);

        return $topic . $partitions;
    }

    /**
     * @return mixed[]
     * @throws ProtocolException
     */
    protected function offsetTopic(string $data): array
    {
        $offset    = 0;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset   += $topicInfo['length'];

        $partitions = $this->decodeArray(substr($data, $offset), [$this, 'offsetPartition']);
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
     * @throws ProtocolException
     */
    protected function offsetPartition(string $data): array
    {
        $offset = 0;

        $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset     += 4;

        $roffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
        $offset += 8;

        $metadata  = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset   += $metadata['length'];
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset   += 2;

        return [
            'length' => $offset,
            'data'   => [
                'partition' => $partitionId,
                'errorCode' => $errorCode,
                'metadata'  => $metadata['data'],
                'offset'    => $roffset,
            ],
        ];
    }
}
