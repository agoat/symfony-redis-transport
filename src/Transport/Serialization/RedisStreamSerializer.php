<?php

namespace Ospa\CloudCore\Transport\Serialization;


use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Stamp\SerializerStamp;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Serializer\Normalizer\ArrayDenormalizer;
use Symfony\Component\Serializer\Normalizer\DateTimeNormalizer;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;
use Symfony\Component\Serializer\Normalizer\PropertyNormalizer;
use Symfony\Component\Serializer\Serializer as SymfonySerializer;
use Symfony\Component\Serializer\SerializerInterface as SymfonySerializerInterface;


class RedisStreamSerializer implements SerializerInterface
{
    private const TYPE_HEADER = 'X-Message-Type';
    private const STAMP_HEADER_PREFIX = 'X-Message-Stamp-';

    private $serializer;
    private $format;
    private $context;

    public function __construct(SymfonySerializerInterface $serializer = null, string $format = 'json', array $context = array())
    {
        $this->serializer = $serializer ?? self::create()->serializer;
        $this->format = $format;
        $this->context = $context;
    }

    public static function create(): self
    {
        if (! class_exists(SymfonySerializer::class)) {
            throw new LogicException(sprintf('The default Messenger Serializer requires Symfony\'s Serializer component. Try running "composer require symfony/serializer".'));
        }

        $encoders = [new JsonEncoder()];
        $normalizers = [new DateTimeNormalizer(), new ArrayDenormalizer(), new ObjectNormalizer()];
        $serializer = new SymfonySerializer($normalizers, $encoders);

        return new self($serializer);
    }

    /**
     * {@inheritdoc}
     */
    public function decode(array $encodedEnvelope): Envelope
    {
        if (empty($encodedEnvelope[self::TYPE_HEADER])) {
            throw new InvalidArgumentException('Encoded envelope does not have a "type" header.');
        }

        $stamps = $this->decodeStamps($encodedEnvelope);
        $message = $this->decodeMessage($encodedEnvelope, $stamps);

        return new Envelope($message, ...$stamps);
    }

    /**
     * {@inheritdoc}
     */
    public function encode(Envelope $envelope): array
    {
        return array(self::TYPE_HEADER => \get_class($envelope->getMessage())) +
            $this->encodeStamps($envelope) +
            $this->encodeMessage($envelope);
    }

    private function decodeMessage(array $encodedEnvelope, $stamps)
    {
        $context = $this->context;
        if (isset($stamps[SerializerStamp::class])) {
            $context = end($stamps[SerializerStamp::class])->getContext() + $context;
        }

        array_walk($encodedEnvelope, function (&$encodedValue, $key) {
            $encodedValue = strpos($key, 'X-Message-') === false ? $this->serializer->decode($encodedValue, $this->format) : $encodedValue;
        });

        return $this->serializer->denormalize($encodedEnvelope, $encodedEnvelope[self::TYPE_HEADER], $this->format, $context);
    }

    private function encodeMessage(Envelope $envelope): array
    {
        $context = $this->context;
        /** @var SerializerStamp|null $serializerStamp */
        if ($serializerStamp = $envelope->last(SerializerStamp::class)) {
            $context = $serializerStamp->getContext() + $context;
        }

        return array_map(function ($var) {
            return $this->serializer->encode($var, $this->format);
        }, $this->serializer->normalize($envelope->getMessage(), $this->format, $context));
    }

    private function decodeStamps(array $encodedEnvelope): array
    {
        $stamps = array();
        foreach ($encodedEnvelope as $name => $value) {
            if (0 !== strpos($name, self::STAMP_HEADER_PREFIX)) {
                continue;
            }

            $stamps[] = $this->serializer->deserialize($value, substr($name, \strlen(self::STAMP_HEADER_PREFIX)).'[]', $this->format, $this->context);
        }
        if ($stamps) {
            $stamps = array_merge(...$stamps);
        }

        return $stamps;
    }

    private function encodeStamps(Envelope $envelope): array
    {
        if (! $allStamps = $envelope->all()) {
            return array();
        }

        $headers = array();
        foreach ($allStamps as $class => $stamps) {
            $headers[self::STAMP_HEADER_PREFIX.$class] = $this->serializer->serialize($stamps, $this->format, $this->context);
        }

        return $headers;
    }
}
