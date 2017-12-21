<?php
namespace Kafka\Config;

use Kafka\Exception;
use Kafka\Contracts\Config\Sasl as SaslInterface;

class Sasl implements SaslInterface
{
    private $keytab    = '';
    private $principal = '';
    private $username  = '';
    private $password  = '';

    public function getKeytab() : string
    {
        return $this->keytab;
    }

    public function getPrincipal() : string
    {
        return $this->principal;
    }

    public function getUsername() : string
    {
        return $this->username;
    }

    public function getPassword() : string
    {
        return $this->password;
    }

    public function setKeytab(string $keytab) : void
    {
        if (! file_exists($keytab) || ! is_file($keytab)) {
            throw new \Kafka\Exception\Config('Set sasl gssapi keytab file is invalid');
        }
        $this->keytab = $keytab;
    }

    public function setPrincipal(string $principal) : void
    {
        $this->principal = $principal;
    }

    public function setUsername(string $username) : void
    {
        $this->username = $username;
    }
    public function setPassword(string $password) : void
    {
        $this->password = $password;
    }
}
