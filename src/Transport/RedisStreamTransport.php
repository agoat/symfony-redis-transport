<?php

namespace Agoat\RedisTransport\Transport;


use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class RedisStreamTransport implements TransportInterface
{
    private $redis;
    private $key;
    private $serializer;
    private $group;
    private $consumer;
    private $timeout;
    private $count;

    /**
     * RedisStreamTransport constructor.
     */
    public function __construct(\Redis $redis, string $namespace, SerializerInterface $serializer, string $channel, string $group, string $consumer, int $timeout, int $count)
    {
        $this->redis = $redis;
        $this->key = $namespace . ':transport:' . $channel; // key = namespace:transport:channel:<priority>
        $this->serializer = $serializer;
        $this->group = $group;
        $this->consumer = $consumer;
        $this->timeout = $timeout;
        $this->count = $count;
    }

    /**
     * @inheritDoc
     */
    public function send(Envelope $envelope): Envelope
    {
        return ($this->sender ?? $this->getSender())->send($envelope);
    }

    /**
     * @inheritDoc
     */
    public function receive(callable $handler): void
    {
        ($this->receiver ?? $this->getReceiver())->receive($handler);
    }

    /**
     * @inheritDoc
     */
    public function stop(): void
    {
        ($this->receiver ?? $this->getReceiver())->stop();
    }

    private function getReceiver()
    {
        return $this->receiver = new RedisStreamReceiver($this->redis, $this->serializer, $this->key, $this->group, $this->consumer, $this->timeout, $this->count);
    }

    private function getSender()
    {
        return $this->sender = new RedisStreamSender($this->redis, $this->serializer, $this->key, $this->group, $this->consumer);
    }
}