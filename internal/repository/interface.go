package repository

import (
	"context"
	"shortly/models"
)

type TxtlyRepository interface {
	CreateShortURL(ctx context.Context, shortUrl models.ShortUrl) (models.ShortUrl, error)
	GetShortURL(ctx context.Context, slug string) (models.UrlMapping, error)
}
