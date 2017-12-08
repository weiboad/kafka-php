<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace Kafka\Sasl;

use Kafka\CommonSocket;
use Kafka\SaslMechanism;
use Kafka\Exception;
use Kafka\Protocol;
use Kafka\Protocol\Protocol as ProtocolTool;

/**
+------------------------------------------------------------------------------
* Kafka sasl provider for gssapi(Kerberos) mechanism
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

final class Gssapi extends Mechanism implements SaslMechanism
{
    // {{{ consts
    
    private const MECHANISM_NAME = "GSSAPI";

    // }}}
    // {{{ members

    private $keytab;

    private $principal;

    private $gssapi;

    // }}}
    // {{{ functions
    // {{{ public function __construct()
    
    /**
     *
     * __construct
     *
     * @access public
     * @return void
     */
    public function __construct(string $keytab, string $principal)
    {
        if (! file_exists($keytab) || ! is_file($keytab)) {
            throw new Exception('Invalid keytab, keytab file not exists.');
        }
        if (! is_readable($keytab)) {
            throw new Exception('Invalid keytab, keytab file disable read.');
        }

        $this->keytab    = $keytab;
        $this->principal = $principal;
    }

    // }}}
    // {{{ public function authenticate()
    
    /**
     *
     * sasl authenticate
     *
     * @access public
     * @return void
     */
    public function authenticate(CommonSocket $socket) : void
    {
        if (! extension_loaded('krb5')) {
            throw new Exception('Extension `krb5` not installed.');
        }
        $this->handShake($socket, $this->getMechanismName());
        $token = $this->initSecurityContext();

        // send token to server and get server token
        $data = ProtocolTool::encodeString($token, ProtocolTool::PACK_INT32);
        $socket->writeBlocking($data);
        $dataLen = ProtocolTool::unpack(ProtocolTool::BIT_B32, $socket->readBlocking(4));
        $stoken  = $socket->readBlocking($dataLen);
        // warp message use server token and send to server authenticate
        $outputMessage = $this->warpToken($stoken);
        $data          = \Kafka\Protocol\Protocol::encodeString($outputMessage, \Kafka\Protocol\Protocol::PACK_INT32);
        $socket->writeBlocking($data);
    }

    // }}}
    // {{{ public function getMechanismName()
    
    /**
     *
     * get sasl authenticate mechanism name
     *
     * @access public
     * @return string
     */
    public function getMechanismName() : string
    {
        return self::MECHANISM_NAME;
    }

    // }}}
    // {{{ private function initSecurityContext()
    
    private function initSecurityContext() : string
    {
        // init krb5 ccache
        $ccache = new \KRB5CCache();
        $ccache->initKeytab($this->principal, $this->keytab);
        $this->gssapi = new \GSSAPIContext();
        $this->gssapi->acquireCredentials($ccache, $this->principal, GSS_C_INITIATE);

        $token = '';
        $ret   = $this->gssapi->initSecContext($this->principal, null, null, null, $token);
        if (! $ret) {
            throw new Exception('Init security context failure.');
        }
        return $token;
    }
    // }}}
    // {{{ private function warpToken()
    
    private function warpToken(string $token) : string
    {
        $message = '';
        $this->gssapi->wrap($token, $message);
        return $message;
    }

    // }}}
    // }}}
}
