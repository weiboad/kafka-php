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
    
        // init krb5 ccache
        $ccache = new \KRB5CCache();
        $ccache->initKeytab($this->principal, $this->keytab);
        $gssapi = new \GSSAPIContext();
        $gssapi->acquireCredentials($ccache, $this->principal, GSS_C_INITIATE);

        $token = '';
        $ret   = $gssapi->initSecContext($this->principal, null, null, null, $token);
        if (! $ret) {
            throw new Exception('Init security context failure.');
        }

        // send token to server and get server token
        $data = ProtocolTool::encodeString($token, ProtocolTool::PACK_INT32);
        $socket->writeBlocking($data);
        $dataLen       = ProtocolTool::unpack(ProtocolTool::BIT_B32, $socket->read(4));
        $stoken        = $socket->readBlocking($dataLen);
        $outputMessage = '';
        // warp message use server token and send to server authenticate
        $gssapi->wrap($stoken, $outputMessage);
        $data = \Kafka\Protocol\Protocol::encodeString($outputMessage, \Kafka\Protocol\Protocol::PACK_INT32);
        $socket->write($data);
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
    // }}}
}
