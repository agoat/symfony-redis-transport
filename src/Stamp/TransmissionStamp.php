<?php

namespace Agoat\RedisTransport\Stamp;


use Symfony\Component\Messenger\Stamp\StampInterface;

class TransmissionStamp implements StampInterface
{
    private $transmissionDate;


    public function __construct()
    {
        $this->transmissionDate = new \DateTimeImmutable('now');
    }

    public function getTransmissionDate(): \DateTimeImmutable
    {
        return $this->transmissionDate;
    }
}