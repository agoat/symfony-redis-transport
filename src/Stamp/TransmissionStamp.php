<?php

namespace Agoat\RedisTransport\Stamp;


use Symfony\Component\Messenger\Stamp\StampInterface;

class TransmissionStamp implements StampInterface
{
    private const DATETIME_FORMAT = \DateTimeImmutable::RFC3339;

    private $transmissionDate;


    public function __construct($transmissionDate = null)
    {
        $this->transmissionDate = $transmissionDate ?? (new \DateTimeImmutable('now'))->format(self::DATETIME_FORMAT);
    }

    public function getTransmissionDate(): \DateTimeImmutable
    {
        return \DateTimeImmutable::createFromFormat(self::DATETIME_FORMAT, $this->transmissionDate);
    }
}
