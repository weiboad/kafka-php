<?php
namespace Kafka\Protocol;

class ApiVersions extends Protocol
{
    /**
     * @param mixed[] $payloads
     */
    public function encode(array $payloads = []): string
    {
        $header = $this->requestHeader('kafka-php', self::API_VERSIONS_REQUEST, self::API_VERSIONS_REQUEST);

        return self::encodeString($header, self::PACK_INT32);
    }

    /**
     * @return mixed[]
     */
    public function decode(string $data): array
    {
        $offset      = 0;
        $errcode     = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset     += 2;
        $apiVersions = $this->decodeArray(substr($data, $offset), [$this, 'apiVersion']);
        $offset     += $apiVersions['length'];

        return [
            'apiVersions' => $apiVersions['data'],
            'errorCode'   => $errcode,
        ];
    }

    /**
     * @return mixed[]
     */
    protected function apiVersion(string $data): array
    {
        $offset     = 0;
        $apiKey     = self::unpack(self::BIT_B16, substr($data, $offset, 2));
        $offset    += 2;
        $minVersion = self::unpack(self::BIT_B16, substr($data, $offset, 2));
        $offset    += 2;
        $maxVersion = self::unpack(self::BIT_B16, substr($data, $offset, 2));
        $offset    += 2;

        return [
            'length' => $offset,
            'data'   => [$apiKey, $minVersion, $maxVersion],
        ];
    }
}
