<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests\Fixture;

use Storm\Aggregate\AggregateRootBehavior;
use Storm\Contracts\Aggregate\AggregateRoot;

/**
 * A small event-sourced aggregate exercising the trait: a business factory that records
 * the creation event, a command that records another, and `apply{Event}` mutations.
 *
 * @implements AggregateRoot<ArticleId>
 */
final class Article implements AggregateRoot
{
    /** @use AggregateRootBehavior<ArticleId> */
    use AggregateRootBehavior;

    private string $title = '';

    public bool $published = false {
        get {
            return $this->published;
        }
    }

    public static function draft(ArticleId $id, string $title): self
    {
        $article = new self($id);
        $article->recordThat(new ArticleDrafted($id->toString(), $title));

        return $article;
    }

    public function publish(): void
    {
        $this->recordThat(new ArticlePublished($this->identity()->toString()));
    }

    public function title(): string
    {
        return $this->title;
    }

    protected function applyArticleDrafted(ArticleDrafted $event): void
    {
        $this->title = $event->title;
    }

    protected function applyArticlePublished(ArticlePublished $event): void
    {
        $this->published = true;
    }
}
