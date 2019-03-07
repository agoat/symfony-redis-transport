<?php


namespace Agoat\RedisTransport\Stamp;


use Symfony\Component\Messenger\Stamp\StampInterface;

class ScheduleStamp implements StampInterface
{
    private $deliverAfter;


    public function __construct(\DateTimeImmutable $date)
    {
        $this->deliverAfter = $date;
    }

    public function getDate(): \DateTimeImmutable
    {
        return $this->deliverAfter;
    }

    public function haveToBeExecuted(): bool
    {
        // TODO check date with now
        return false;
    }



}