<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests\Fixture;

use Storm\Aggregate\AggregateRootBehavior;
use Storm\Aggregate\Exception\InvalidSnapshotState;
use Storm\Aggregate\SnapshotBehavior;
use Storm\Contracts\Aggregate\SnapshotableAggregateRoot;

/**
 * A SnapshotArticle variant whose `restoreState()` validates strictly, the contract-abiding shape:
 * a missing or mistyped field is a corrupt cache row, declared via `InvalidSnapshotState` so the
 * snapshot repository converts it into a cache miss instead of coercing garbage state.
 *
 * @implements SnapshotableAggregateRoot<ArticleId>
 *
 * @see SnapshotArticle the lenient sibling
 */
final class PickySnapshotArticle implements SnapshotableAggregateRoot
{
    /** @use AggregateRootBehavior<ArticleId> */
    use AggregateRootBehavior;

    use SnapshotBehavior;

    private string $title = '';

    private bool $published = false;

    public static function draft(ArticleId $id, string $title): self
    {
        $article = new self($id);
        $article->recordThat(new ArticleDrafted($id->toString(), $title));

        return $article;
    }

    public function toSnapshot(): array
    {
        return [
            self::SNAPSHOT_VERSION_KEY => self::currentSnapshotVersion(),
            'title' => $this->title,
            'published' => $this->published,
        ];
    }

    protected function restoreState(array $state): void
    {
        if (! isset($state['title']) || ! is_string($state['title'])) {
            throw InvalidSnapshotState::missingField(self::class, 'title');
        }

        if (! isset($state['published']) || ! is_bool($state['published'])) {
            throw InvalidSnapshotState::missingField(self::class, 'published');
        }

        $this->title = $state['title'];
        $this->published = $state['published'];
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
