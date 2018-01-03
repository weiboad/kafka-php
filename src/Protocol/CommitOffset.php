<?php
declare(strict_types=1);

namespace Kafka\Protocol;

use Kafka\Exception\NotSupported;
use Kafka\Exception\Protocol as ProtocolException;

class CommitOffset extends Protocol
{
    /**
     * @param mixed[] $payloads
     *
     * @throws ProtocolException
     * @throws NotSupported
     */
    public function encode(array $payloads = []): string
    {
        if (! isset($payloads['group_id'])) {
            throw new ProtocolException('given commit offset data invalid. `group_id` is undefined.');
        }

        if (! isset($payloads['data'])) {
            throw new ProtocolException('given commit data invalid. `data` is undefined.');
        }

        if (! isset($payloads['generation_id'])) {
            $payloads['generation_id'] = -1;
        }

        if (! isset($payloads['member_id'])) {
            $payloads['member_id'] = '';
        }

        if (! isset($payloads['retention_time'])) {
            $payloads['retention_time'] = -1;
        }

        $version = $this->getApiVersion(self::OFFSET_COMMIT_REQUEST);
        $header  = $this->requestHeader('kafka-php', self::OFFSET_COMMIT_REQUEST, self::OFFSET_COMMIT_REQUEST);

        $data = self::encodeString($payloads['group_id'], self::PACK_INT16);

        if ($version === self::API_VERSION1) {
            $data .= self::pack(self::BIT_B32, (string) $payloads['generation_id']);
            $data .= self::encodeString($payloads['member_id'], self::PACK_INT16);
        }

        if ($version === self::API_VERSION2) {
            $data .= self::pack(self::BIT_B32, (string) $payloads['generation_id']);
            $data .= self::encodeString($payloads['member_id'], self::PACK_INT16);
            $data .= self::pack(self::BIT_B64, (string) $payloads['retention_time']);
        }

        $data .= self::encodeArray($payloads['data'], [$this, 'encodeTopic']);
        $data  = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    /**
     * @return mixed[]
     */
    public function decode(string $data): array
    {
        $offset  = 0;
        $topics  = $this->decodeArray(substr($data, $offset), [$this, 'decodeTopic']);
        $offset += $topics['length'];

        return $topics['data'];
    }

    /**
     * @param mixed[] $values
     *
     * @throws NotSupported
     * @throws ProtocolException
     */
    protected function encodeTopic(array $values): string
    {
        if (! isset($values['topic_name'])) {
            throw new ProtocolException('given commit offset data invalid. `topic_name` is undefined.');
        }
        if (! isset($values['partitions'])) {
            throw new ProtocolException('given commit offset data invalid. `partitions` is undefined.');
        }

        $data  = self::encodeString($values['topic_name'], self::PACK_INT16);
        $data .= self::encodeArray($values['partitions'], [$this, 'encodePartition']);

        return $data;
    }

    /**
     * @param mixed[] $values
     *
     * @throws NotSupported
     * @throws ProtocolException
     */
    protected function encodePartition(array $values): string
    {
        if (! isset($values['partition'])) {
            throw new ProtocolException('given commit offset data invalid. `partition` is undefined.');
        }

        if (! isset($values['offset'])) {
            throw new ProtocolException('given commit offset data invalid. `offset` is undefined.');
        }

        if (! isset($values['metadata'])) {
            $values['metadata'] = '';
        }

        if (! isset($values['timestamp'])) {
            $values['timestamp'] = time() * 1000;
        }

        $version = $this->getApiVersion(self::OFFSET_COMMIT_REQUEST);

        $data  = self::pack(self::BIT_B32, (string) $values['partition']);
        $data .= self::pack(self::BIT_B64, (string) $values['offset']);

        if ($version === self::API_VERSION1) {
            $data .= self::pack(self::BIT_B64, (string) $values['timestamp']);
        }

        $data .= self::encodeString($values['metadata'], self::PACK_INT16);

        return $data;
    }

    /**
     * @return mixed[]
     */
    protected function decodeTopic(string $data): array
    {
        $offset    = 0;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset   += $topicInfo['length'];

        $partitions = $this->decodeArray(substr($data, $offset), [$this, 'decodePartition']);
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
     */
    protected function decodePartition(string $data): array
    {
        $offset = 0;

        $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset     += 4;

        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset   += 2;

        return [
            'length' => $offset,
            'data'   => [
                'partition' => $partitionId,
                'errorCode' => $errorCode,
            ],
        ];
    }
}
