<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests\Fixture;

use Storm\Contracts\Message\DomainEvent;

final class ArticlePublished implements DomainEvent
{
    public function __construct(
        public string $articleId,
    ) {}

    public function aggregateId(): string
    {
        return $this->articleId;
    }

    public function toPayload(): array
    {
        return ['article_id' => $this->articleId];
    }

    public static function fromPayload(array $payload): static
    {
        return new self((string) $payload['article_id']);
    }
}
