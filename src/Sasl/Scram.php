<?php
namespace Kafka\Sasl;

use Kafka\connections\CommonSocket;
use Kafka\SaslMechanism;
use Kafka\Exception;
use Kafka\Protocol;
use Kafka\Protocol\Protocol as ProtocolTool;

class Scram extends Mechanism implements SaslMechanism
{

    const SCRAM_SHA_256 = 256;
    const SCRAM_SHA_512 = 512;
    
    const MECHANISM_NAME = "SCRAM-SHA-";

    private static $allowShaAlgorithm = [
        self::SCRAM_SHA_256 => 'sha256',
        self::SCRAM_SHA_512 => 'sha512',
    ];

    private $hashAlgorithm;

    private $username;

    private $password;

    private $cnonce;

    private $firstMessageBare;

    private $saltedPassword;

    private $authMessage;

    
    /**
     *
     * __construct
     *
     * @access public
     * @return void
     */
    public function __construct($username, $password, $algorithm)
    {
        if (! isset(self::$allowShaAlgorithm[$algorithm])) {
            throw new Exception('Invalid hash algorithm given, it must be one of: [SCRAM_SHA_256, SCRAM_SHA_512].');
        }
        $this->hashAlgorithm = $algorithm;
        $this->username      = $this->formatName(trim($username));
        $this->password      = trim($password);
    }

    
    /**
     *
     * sasl authenticate
     *
     * @access protected
     * @return void
     */
    protected function performAuthentication(CommonSocket $socket)
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

    
    /**
     *
     * get sasl authenticate mechanism name
     *
     * @access public
     * @return string
     */
    public function getName()
    {
        return self::MECHANISM_NAME . $this->hashAlgorithm;
    }

    /**
      * Generate the initial response which can be either sent directly in the first message or as a response to an empty
      *
      * @return string The SCRAM response to send.
      * @access private
      */
    protected function firstMessage()
    {
        $message                = '';
        $this->cnonce           = $this->generateNonce();
        $message                = sprintf('n,,n=%s,r=%s', $this->username, $this->cnonce);
        $this->firstMessageBare = substr($message, 3);
        return $message;
    }

    /**
      * Generate the final message
      *
      * @return string The SCRAM response to send.
      * @access private
      */
    protected function finalMessage($challenge)
    {
        $message        = '';
        $challengeArray = explode(',', $challenge);
        if (count($challengeArray) < 3) {
            throw new Exception('Server response challenge is invalid.');
        }

        $nonce = substr($challengeArray[0], 2);
        $salt  = base64_decode(substr($challengeArray[1], 2));
        if (! $salt) {
            throw new Exception('Server response challenge is invalid, paser salt is failure.');
        }

        $i      = intval(substr($challengeArray[2], 2));
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
      * @return bool Whether the server has been authenticated.
      * @access protected
      */
    protected function verifyMessage($data)
    {
        $verifierRegexp = '#^v=((?:[A-Za-z0-9/+]{4})*(?:[A-Za-z0-9]{3}=|[A-Xa-z0-9]{2}==)?)$#';
        if ($this->saltedPassword === null || $this->authMessage === null) {
            return false;
        }

        if (! preg_match($verifierRegexp, $data, $matches)) {
            return false;
        }

        $proposedServerSignature = base64_decode($matches[1]);
        $serverKey               = $this->hmac($this->saltedPassword, "Server Key", true);
        $serverSignature         = $this->hmac($serverKey, $this->authMessage, true);
        return hash_equals($proposedServerSignature, $serverSignature);
    }

  /**
    * Creates the client nonce for the response
    *
    * @return string
    * @access protected
    */
    protected function generateNonce()
    {
        $str = '';
        for ($i=0; $i<32; $i++) {
            $str .= chr(mt_rand(0, 255));
        }
        return base64_encode($str);
    }

    private function hash($data)
    {
        return \hash(self::$allowShaAlgorithm[$this->hashAlgorithm], $data, true);
    }

    private function hmac($key, $data, $raw)
    {
        return \hash_hmac(self::$allowShaAlgorithm[$this->hashAlgorithm], $data, $key, $raw);
    }

  /**
    * Prepare a name for inclusion in a SCRAM response.
    * @See RFC-4013.
    *
    * @param string $user a name to be prepared.
    * @return string the reformated name.
    * @access private
    */
    private function formatName($user)
    {
        return str_replace(['=', ','], ['=3D', '=2C'], $user);

        return $user;
    }

  /**
    * Hi() call, which is essentially PBKDF2 (RFC-2898) with HMAC-H() as the pseudorandom function.
    *
    * @param string $str The string to hash.
    * @param string $hash The hash value.
    * @param int $i The iteration count.
    * @return string
    * @access private
    */
    private function hi($str, $salt, $icnt)
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
