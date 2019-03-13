<?php


namespace Agoat\RedisTransport\Transport;


use Agoat\RedisTransport\Stamp\HighPriorityStamp;
use Agoat\RedisTransport\Stamp\ScheduleStamp;
use Agoat\RedisTransport\Stamp\TransmissionStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class RedisStreamSender implements SenderInterface
{
    private $redis;
    private $serializer;
    private $key;
    private $group;
    private $consumer;

    public function __construct(\Redis $redis, SerializerInterface $serializer, string $key, string $group, string $consumer)
    {
        $this->redis = $redis;
        $this->serializer = $serializer;
        $this->key = $key;
        $this->group = $group;
        $this->consumer = $consumer;
    }

    /**
     * @inheritDoc
     */
    public function send(Envelope $envelope): Envelope
    {
        if ($encodedMessage = $this->serializer->encode($envelope->with(new TransmissionStamp()))) {
            if (null !== $scheduleStamp = $envelope->last(ScheduleStamp::class)) {
                $id = 'message-' . microtime(true) * 10000;

                $this->redis->hMSet($this->key . ':data:' . $id, $encodedMessage);
                $this->redis->zAdd($this->key . ':schedule', $scheduleStamp->getDate()->getTimestamp(), $id);

                return $envelope;
            }

            $priority = (null !== $envelope->last(HighPriorityStamp::class)) ? 'high' : 'default';
            $success = $this->redis->xAdd($this->key . ':' . $priority, '*', $encodedMessage);

            if (! $success) {
                // TODO throw exception
            }

            return $envelope;
        }
    }
}
