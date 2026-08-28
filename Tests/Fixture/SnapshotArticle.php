<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests\Fixture;

use Storm\Aggregate\AggregateRootBehavior;
use Storm\Aggregate\SnapshotBehavior;
use Storm\Contracts\Aggregate\SnapshotableAggregateRoot;

/**
 * A snapshotable variant of Article that exercises SnapshotBehavior: state export/restore,
 * post-snapshot replay, and the `_snapshot_version` discard.
 *
 * @implements SnapshotableAggregateRoot<ArticleId>
 *
 * @see Article
 */
final class SnapshotArticle implements SnapshotableAggregateRoot
{
    /** @use AggregateRootBehavior<ArticleId> */
    use AggregateRootBehavior;

    use SnapshotBehavior;

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
        $this->title = (string) $state['title'];
        $this->published = (bool) $state['published'];
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
