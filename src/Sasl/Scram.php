<?php
declare(strict_types=1);

namespace Kafka\Sasl;

use Kafka\CommonSocket;
use Kafka\Exception;
use Kafka\Protocol\Protocol as ProtocolTool;
use function base64_decode;
use function base64_encode;
use function chr;
use function count;
use function explode;
use function hash;
use function hash_equals;
use function hash_hmac;
use function preg_match;
use function random_int;
use function sprintf;
use function str_replace;
use function strlen;
use function substr;
use function trim;

class Scram extends Mechanism
{
    public const SCRAM_SHA_256 = 256;
    public const SCRAM_SHA_512 = 512;

    private const MECHANISM_NAME = 'SCRAM-SHA-';

    private const ALLOW_SHA_ALGORITHM = [
        self::SCRAM_SHA_256 => 'sha256',
        self::SCRAM_SHA_512 => 'sha512',
    ];

    /**
     * @var int
     */
    private $hashAlgorithm;

    /**
     * @var string
     */
    private $username;

    /**
     * @var string
     */
    private $password;

    /**
     * @var string
     */
    private $cnonce;

    /**
     * @var string
     */
    private $firstMessageBare;

    /**
     * @var string
     */
    private $saltedPassword;

    /**
     * @var string
     */
    private $authMessage;

    /**
     * @throws \Kafka\Exception
     */
    public function __construct(string $username, string $password, int $algorithm)
    {
        if (! isset(self::ALLOW_SHA_ALGORITHM[$algorithm])) {
            throw new Exception('Invalid hash algorithm given, it must be one of: [SCRAM_SHA_256, SCRAM_SHA_512].');
        }

        $this->hashAlgorithm = $algorithm;
        $this->username      = $this->formatName(trim($username));
        $this->password      = trim($password);
    }

    /**
     * @throws \Kafka\Exception
     * @throws \Exception
     */
    protected function performAuthentication(CommonSocket $socket): void
    {
        $firstMessage = $this->firstMessage();
        $data         = ProtocolTool::encodeString($firstMessage, ProtocolTool::PACK_INT32);
        $socket->writeBlocking($data);
        $dataLen           = ProtocolTool::unpack(ProtocolTool::BIT_B32, $socket->readBlocking(4));
        $serverFistMessage = $socket->readBlocking($dataLen);

        $finalMessage = $this->finalMessage($serverFistMessage);
        $data         = ProtocolTool::encodeString($finalMessage, ProtocolTool::PACK_INT32);
        $socket->writeBlocking($data);
        $dataLen       = ProtocolTool::unpack(ProtocolTool::BIT_B32, $socket->readBlocking(4));
        $verifyMessage = $socket->readBlocking($dataLen);

        if (! $this->verifyMessage($verifyMessage)) {
            throw new Exception('Verify server final response message is failure');
        }
    }


    public function getName(): string
    {
        return self::MECHANISM_NAME . $this->hashAlgorithm;
    }

    /**
     * @throws \Exception
     */
    protected function firstMessage(): string
    {
        $this->cnonce = $this->generateNonce();
        $message      = sprintf('n,,n=%s,r=%s', $this->username, $this->cnonce);

        $this->firstMessageBare = substr($message, 3);

        return $message;
    }

    /**
     * @throws \Kafka\Exception
     */
    protected function finalMessage(string $challenge): string
    {
        $challengeArray = explode(',', $challenge);

        if (count($challengeArray) < 3) {
            throw new Exception('Server response challenge is invalid.');
        }

        $nonce = substr($challengeArray[0], 2);
        $salt  = base64_decode(substr($challengeArray[1], 2));

        if (! $salt) {
            throw new Exception('Server response challenge is invalid, paser salt is failure.');
        }

        $i      = (int) substr($challengeArray[2], 2);
        $cnonce = substr($nonce, 0, strlen($this->cnonce));

        if ($cnonce !== $this->cnonce) {
            throw new Exception('Server response challenge is invalid, cnonce is invalid.');
        }

        $finalMessage = 'c=biws,r=' . $nonce; // `biws` is base64 encode of "n,,"

        /* Constructing the ClientProof attribute (p):
         *
         * p = Base64-encoded ClientProof
         * SaltedPassword  := Hi(Normalize(password), salt, i)
         * ClientKey       := HMAC(SaltedPassword, "Client Key")
         * StoredKey       := H(ClientKey)
         * AuthMessage     := client-first-message-bare + "," +
         *                    server-first-message + "," +
         *                    client-final-message-without-proof
         * ClientSignature := HMAC(StoredKey, AuthMessage)
         * ClientProof     := ClientKey XOR ClientSignature
         * ServerKey       := HMAC(SaltedPassword, "Server Key")
         * ServerSignature := HMAC(ServerKey, AuthMessage)
         */

        $saltedPassword       = $this->hi($this->password, $salt, $i);
        $this->saltedPassword = $saltedPassword;
        $clientKey            = $this->hmac($saltedPassword, 'Client Key', true);
        $storedKey            = $this->hash($clientKey);
        $authMessage          = $this->firstMessageBare . ',' . $challenge . ',' . $finalMessage;
        $this->authMessage    = $authMessage;

        $clientSignature = $this->hmac($storedKey, $authMessage, true);
        $clientProof     = $clientKey ^ $clientSignature;
        $proof           = ',p=' . base64_encode($clientProof);

        return $finalMessage . $proof;
    }

    /**
     * SCRAM has also a server verification step
     *
     * @param string $data The additional data sent along a successful outcome.
     *
     * @return bool Whether the server has been authenticated.
     */
    protected function verifyMessage(string $data): bool
    {
        $verifierRegexp = '#^v=((?:[A-Za-z0-9/+]{4})*(?:[A-Za-z0-9]{3}=|[A-Xa-z0-9]{2}==)?)$#';

        if ($this->saltedPassword === null || $this->authMessage === null) {
            return false;
        }

        if (! preg_match($verifierRegexp, $data, $matches)) {
            return false;
        }

        $proposedServerSignature = base64_decode($matches[1]);
        $serverKey               = $this->hmac($this->saltedPassword, 'Server Key', true);
        $serverSignature         = $this->hmac($serverKey, $this->authMessage, true);
        return hash_equals($proposedServerSignature, $serverSignature);
    }

    /**
     * @throws \Exception
     */
    protected function generateNonce(): string
    {
        $str = '';

        for ($i = 0; $i < 32; $i++) {
            $str .= chr(random_int(0, 255));
        }

        return base64_encode($str);
    }

    private function hash(string $data): string
    {
        return hash(self::ALLOW_SHA_ALGORITHM[$this->hashAlgorithm], $data, true);
    }

    private function hmac(string $key, string $data, bool $raw): string
    {
        return hash_hmac(self::ALLOW_SHA_ALGORITHM[$this->hashAlgorithm], $data, $key, $raw);
    }

    /**
     * Prepare a name for inclusion in a SCRAM response.
     * @See RFC-4013.
     *
     * @param string $user a name to be prepared.
     * @return string the reformatted name.
     */
    private function formatName(string $user): string
    {
        return str_replace(['=', ','], ['=3D', '=2C'], $user);
    }

    /**
     * Hi() call, which is essentially PBKDF2 (RFC-2898) with HMAC-H() as the pseudorandom function.
     */
    private function hi(string $str, string $salt, int $icnt): string
    {
        $int1   = "\0\0\0\1";
        $ui     = $this->hmac($str, $salt . $int1, true);
        $result = $ui;

        for ($k = 1; $k < $icnt; $k++) {
            $ui     = $this->hmac($str, $ui, true);
            $result = $result ^ $ui;
        }

        return $result;
    }
}
