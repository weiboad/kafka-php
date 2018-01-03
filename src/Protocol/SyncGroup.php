<?php
declare(strict_types=1);

namespace Kafka\Protocol;

use Kafka\Exception\NotSupported;
use Kafka\Exception\Protocol as ProtocolException;

class SyncGroup extends Protocol
{
    /**
     * @param mixed[] $payloads
     *
     * @throws NotSupported
     * @throws ProtocolException
     */
    public function encode(array $payloads = []): string
    {
        if (! isset($payloads['group_id'])) {
            throw new ProtocolException('given sync group data invalid. `group_id` is undefined.');
        }

        if (! isset($payloads['generation_id'])) {
            throw new ProtocolException('given sync group data invalid. `generation_id` is undefined.');
        }

        if (! isset($payloads['member_id'])) {
            throw new ProtocolException('given sync group data invalid. `member_id` is undefined.');
        }

        if (! isset($payloads['data'])) {
            throw new ProtocolException('given sync group data invalid. `data` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', self::SYNC_GROUP_REQUEST, self::SYNC_GROUP_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data  .= self::pack(self::BIT_B32, (string) $payloads['generation_id']);
        $data  .= self::encodeString($payloads['member_id'], self::PACK_INT16);
        $data  .= self::encodeArray($payloads['data'], [$this, 'encodeGroupAssignment']);

        return self::encodeString($header . $data, self::PACK_INT32);
    }

    /**
     * @return mixed[]
     */
    public function decode(string $data): array
    {
        $offset    = 0;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset   += 2;

        $memberAssignments = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset           += $memberAssignments['length'];

        $memberAssignment = $memberAssignments['data'];

        if ($memberAssignment === '') {
            return [
                'errorCode' => $errorCode,
            ];
        }

        $memberAssignmentOffset  = 0;
        $version                 = self::unpack(
            self::BIT_B16_SIGNED,
            substr($memberAssignment, $memberAssignmentOffset, 2)
        );
        $memberAssignmentOffset += 2;
        $partitionAssignments    = $this->decodeArray(
            substr($memberAssignment, $memberAssignmentOffset),
            [$this, 'syncGroupResponsePartition']
        );
        $memberAssignmentOffset += $partitionAssignments['length'];
        $userData                = $this->decodeString(
            substr($memberAssignment, $memberAssignmentOffset),
            self::BIT_B32
        );

        return [
            'errorCode'            => $errorCode,
            'partitionAssignments' => $partitionAssignments['data'],
            'version'              => $version,
            'userData'             => $userData['data'],
        ];
    }

    /**
     * @param mixed[] $values
     *
     * @throws NotSupported
     * @throws ProtocolException
     */
    protected function encodeGroupAssignment(array $values): string
    {
        if (! isset($values['version'])) {
            throw new ProtocolException('given data invalid. `version` is undefined.');
        }

        if (! isset($values['member_id'])) {
            throw new ProtocolException('given data invalid. `member_id` is undefined.');
        }

        if (! isset($values['assignments'])) {
            throw new ProtocolException('given data invalid. `assignments` is undefined.');
        }

        if (! isset($values['user_data'])) {
            $values['user_data'] = '';
        }

        $memberId = self::encodeString($values['member_id'], self::PACK_INT16);

        $data  = self::pack(self::BIT_B16, '0');
        $data .= self::encodeArray($values['assignments'], [$this, 'encodeGroupAssignmentTopic']);
        $data .= self::encodeString($values['user_data'], self::PACK_INT32);

        return $memberId . self::encodeString($data, self::PACK_INT32);
    }

    /**
     * @param mixed[] $values
     *
     * @throws ProtocolException
     * @throws NotSupported
     */
    protected function encodeGroupAssignmentTopic(array $values): string
    {
        if (! isset($values['topic_name'])) {
            throw new ProtocolException('given data invalid. `topic_name` is undefined.');
        }

        if (! isset($values['partitions'])) {
            throw new ProtocolException('given data invalid. `partitions` is undefined.');
        }

        $topicName  = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], [$this, 'encodeGroupAssignmentTopicPartition']);

        return $topicName . $partitions;
    }

    protected function encodeGroupAssignmentTopicPartition(int $values): string
    {
        return self::pack(self::BIT_B32, (string) $values);
    }

    /**
     * @return mixed[]
     */
    protected function syncGroupResponsePartition(string $data): array
    {
        $offset     = 0;
        $topicName  = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset    += $topicName['length'];
        $partitions = $this->decodePrimitiveArray(substr($data, $offset), self::BIT_B32);
        $offset    += $partitions['length'];

        return [
            'length' => $offset,
            'data'   => [
                'topicName'  => $topicName['data'],
                'partitions' => $partitions['data'],
            ],
        ];
    }
}
