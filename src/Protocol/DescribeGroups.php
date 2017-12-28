<?php

namespace Kafka\Protocol;

use Kafka\Exception\NotSupported;

class DescribeGroups extends Protocol
{
    /**
     * @param mixed[] $payloads
     *
     * @throws NotSupported
     */
    public function encode(array $payloads = []): string
    {
        $header = $this->requestHeader('kafka-php', self::DESCRIBE_GROUPS_REQUEST, self::DESCRIBE_GROUPS_REQUEST);
        $data   = self::encodeArray($payloads, [$this, 'encodeString'], self::PACK_INT16);

        return self::encodeString($header . $data, self::PACK_INT32);
    }

    /**
     * @return mixed[]
     */
    public function decode(string $data): array
    {
        $offset  = 0;
        $groups  = $this->decodeArray(substr($data, $offset), [$this, 'describeGroup']);
        $offset += $groups['length'];

        return $groups['data'];
    }

    /**
     * @return mixed[]
     */
    protected function describeGroup(string $data): array
    {
        $offset       = 0;
        $errorCode    = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset      += 2;
        $groupId      = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset      += $groupId['length'];
        $state        = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset      += $state['length'];
        $protocolType = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset      += $protocolType['length'];
        $protocol     = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset      += $protocol['length'];

        $members = $this->decodeArray(substr($data, $offset), [$this, 'describeMember']);
        $offset += $members['length'];

        return [
            'length' => $offset,
            'data'   => [
                'errorCode'    => $errorCode,
                'groupId'      => $groupId['data'],
                'state'        => $state['data'],
                'protocolType' => $protocolType['data'],
                'protocol'     => $protocol['data'],
                'members'      => $members['data'],
            ],
        ];
    }

    /**
     * @return mixed[]
     */
    protected function describeMember(string $data): array
    {
        $offset     = 0;
        $memberId   = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset    += $memberId['length'];
        $clientId   = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset    += $clientId['length'];
        $clientHost = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset    += $clientHost['length'];
        $metadata   = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset    += $metadata['length'];
        $assignment = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset    += $assignment['length'];

        $memberAssignment        = $assignment['data'];
        $memberAssignmentOffset  = 0;
        $version                 = self::unpack(self::BIT_B16_SIGNED, substr($memberAssignment, $memberAssignmentOffset, 2));
        $memberAssignmentOffset += 2;
        $partitionAssignments    = $this->decodeArray(substr($memberAssignment, $memberAssignmentOffset), [$this, 'describeResponsePartition']);
        $memberAssignmentOffset += $partitionAssignments['length'];
        $userData                = $this->decodeString(substr($memberAssignment, $memberAssignmentOffset), self::BIT_B32);

        $metaData     = $metadata['data'];
        $metaOffset   = 0;
        $version      = self::unpack(self::BIT_B16, substr($metaData, $metaOffset, 2));
        $metaOffset  += 2;
        $topics       = $this->decodeArray(substr($metaData, $metaOffset), [$this, 'decodeString'], self::BIT_B16);
        $metaOffset  += $topics['length'];
        $metaUserData = $this->decodeString(substr($metaData, $metaOffset), self::BIT_B32);

        return [
            'length' => $offset,
            'data'   => [
                'memberId'   => $memberId['data'],
                'clientId'   => $clientId['data'],
                'clientHost' => $clientHost['data'],
                'metadata'   => [
                    'version'  => $version,
                    'topics'   => $topics['data'],
                    'userData' => $metaUserData['data'],
                ],
                'assignment' => [
                    'version'    => $version,
                    'partitions' => $partitionAssignments['data'],
                    'userData'   => $userData['data'],
                ],
            ],
        ];
    }

    /**
     * @return mixed[]
     */
    protected function describeResponsePartition(string $data): array
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
