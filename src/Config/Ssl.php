<?php
namespace Kafka\Config;

use Kafka\Exception;
use Kafka\Contracts\Config\Ssl as SslInterface;

class Ssl implements SslInterface
{
    private $localCert  = '';
    private $localPk    = '';
    private $verifyPeer = false;
    private $passphrase = '';
    private $cafile     = '';
    private $peerName   = '';
    private $enable     = false;

    public function getLocalCert() : string
    {
        return $this->localCert;
    }

    public function getLocalPk() : string
    {
        return $this->localPk;
    }

    public function getVerifyPeer() : bool
    {
        return $this->verifyPeer;
    }

    public function getPassphrase() : string
    {
        return $this->passphrase;
    }

    public function getCafile() : string
    {
        return $this->cafile;
    }

    public function getPeerName() : string
    {
        return $this->peerName;
    }

    public function getEnable() : bool
    {
        return $this->enable;
    }

    public function setEnable(bool $enable) : void
    {
        $this->enable = $enable;
    }

    public function setVerifyPeer(bool $verifyPeer) : void
    {
        $this->verifyPeer = $verifyPeer;
    }

    public function setPassphrase(string $passphrase) : void
    {
        $this->passphrase = $passphrase;
    }

    public function setPeerName(string $peerName) : void
    {
        $this->peerName = $peerName;
    }

    public function setLocalCert(string $localCert) : void
    {
        if (! file_exists($localCert) || ! is_file($localCert)) {
            throw new \Kafka\Exception\Config('Set ssl local cert file is invalid');
        }
        $this->localCert = $localCert;
    }

    public function setLocalPk(string $localPk) : void
    {
        if (! file_exists($localPk) || ! is_file($localPk)) {
            throw new \Kafka\Exception\Config('Set ssl local private key file is invalid');
        }
        $this->localPk = $localPk;
    }

    public function setCafile(string $cafile) : void
    {
        if (! file_exists($cafile) || ! is_file($cafile)) {
            throw new \Kafka\Exception\Config('Set ssl ca file is invalid');
        }
        $this->cafile = $cafile;
    }
}
