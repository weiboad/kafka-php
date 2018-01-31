<?php
namespace Kafka\Sasl;

use Kafka\CommonSocket;
use Kafka\SaslMechanism;
use Kafka\Exception;
use Kafka\Protocol;
use Kafka\Protocol\Protocol as ProtocolTool;

class Gssapi extends Mechanism
{
    const MECHANISM_NAME = "GSSAPI";

    private $principal;

    private $gssapi;
    
    private static $ccache;

    /**
     *
     * __construct
     *
     * @access public
     * @return void
     */
    public function __construct(\GSSAPIContext $gssapi, $principal)
    {
        $this->gssapi    = $gssapi;
        $this->principal = $principal;
    }

    public static function fromKeytab($keytab, $principal)
    {
        if (! extension_loaded('krb5')) {
            throw new Exception('Extension "krb5" is required for "GSSAPI" authentication');
        }

        if (! file_exists($keytab) || ! is_file($keytab)) {
            throw new Exception('Invalid keytab, keytab file not exists.');
        }

        if (! is_readable($keytab)) {
            throw new Exception('Invalid keytab, keytab file disable read.');
        }
        
        self::$ccache = new \KRB5CCache();
        self::$ccache->initKeytab($principal, $keytab);

        $gssapi = new \GSSAPIContext();
        $gssapi->acquireCredentials(self::$ccache, $principal, \GSS_C_INITIATE);
        return new self($gssapi, $principal);
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
        $token = $this->initSecurityContext();

        // send token to server and get server token
        $data = ProtocolTool::encodeString($token, ProtocolTool::PACK_INT32);
        $socket->writeBlocking($data);
        $dataLen = ProtocolTool::unpack(ProtocolTool::BIT_B32, $socket->readBlocking(4));
        $stoken  = $socket->readBlocking($dataLen);
        // warp message use server token and send to server authenticate
        $outputMessage = $this->wrapToken($stoken);
        $data          = \Kafka\Protocol\Protocol::encodeString($outputMessage, \Kafka\Protocol\Protocol::PACK_INT32);
        $socket->writeBlocking($data);
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
        return self::MECHANISM_NAME;
    }

    private function initSecurityContext()
    {
        $token = '';
        $ret   = $this->gssapi->initSecContext($this->principal, null, null, null, $token);
        if (! $ret) {
            throw new Exception('Init security context failure.');
        }
        return $token;
    }
    
    private function wrapToken($token)
    {
        $message = '';
        $this->gssapi->wrap($token, $message);
        return $message;
    }
}
