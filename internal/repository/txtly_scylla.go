package repository

import (
	"context"
	"errors"
	"fmt"
	"shortly/models"
	"time"

	"github.com/gocql/gocql"
)

const defaultUrlMappingTable = "url_mappings"
const defaultcompanyUrlCreatedTable = "company_urls_created"
const defaultcompanyUrlExpiryTable = "company_urls_expiry"

var ErrSlugNotFound = errors.New("slug mapping not found")

type ScyllaTxtlyRepository struct {
	session *gocql.Session
}

func NewScyllaTxtlyRepository(session *gocql.Session) *ScyllaTxtlyRepository {
	return &ScyllaTxtlyRepository{
		session: session,
	}
}

func (r *ScyllaTxtlyRepository) insertURLMappingCQL() string {
	return fmt.Sprintf(`
		INSERT INTO %s (
			slug,
			long_url,
			channel,
			company_id
		) VALUES (?, ?, ?,?)
	`, defaultUrlMappingTable)
}

func (r *ScyllaTxtlyRepository) insertCompanyUrlsCreatedCQL() string {
	return fmt.Sprintf(`
		INSERT INTO %s (
			company_id,
			bucket,
			created,
			slug
		) VALUES (?, ?, ?,?)
	`, defaultcompanyUrlCreatedTable)
}

func (r *ScyllaTxtlyRepository) insertCompanyUrlsExpiryCQL() string {
	return fmt.Sprintf(`
		INSERT INTO %s (
			company_id,
			expiry,
			slug
		) VALUES (?,?,?)
	`, defaultcompanyUrlCreatedTable)
}

func (r *ScyllaTxtlyRepository) getBySlug() string {
	return fmt.Sprintf(`
		SELECT
			*
		FROM %s
		WHERE slug = ?
	`, defaultUrlMappingTable)
}

func (r *ScyllaTxtlyRepository) CreateShortURL(ctx context.Context, shortUrl models.ShortUrl) (models.ShortUrl, error) {

	if shortUrl.CompanyId == "" {
		return models.ShortUrl{}, fmt.Errorf("company_id not present")
	}

	now := time.Now().UTC()
	bucket := fmt.Sprintf("%d-%d", now.Year(), int(now.Month()))

	if shortUrl.Created.IsZero() {
		shortUrl.Created = now
	}

	batch := r.session.Batch(gocql.LoggedBatch).WithContext(ctx)

	batch.Query(r.insertURLMappingCQL(), shortUrl.Slug, shortUrl.LongUrl, shortUrl.CompanyId, shortUrl.Channel)
	batch.Query(r.insertCompanyUrlsCreatedCQL(), shortUrl.CompanyId, bucket, shortUrl.Created, shortUrl.Slug)
	batch.Query(r.insertCompanyUrlsExpiryCQL(), shortUrl.CompanyId, shortUrl.Expiry, shortUrl.Slug)

	if err := r.session.ExecuteBatch(batch); err != nil {
		return models.ShortUrl{}, fmt.Errorf("insert cql error : %v", err)
	}

	return shortUrl, nil

}

func (r *ScyllaTxtlyRepository) GetShortURL(ctx context.Context, slug string) (models.UrlMapping, error) {

	urlMapping := models.UrlMapping{}
	if err := r.session.Query(r.getBySlug(), slug).WithContext(ctx).Scan(&urlMapping); err != nil {
		if err == gocql.ErrNotFound {
			return models.UrlMapping{}, ErrSlugNotFound
		}
		return models.UrlMapping{}, err
	}

	return urlMapping, nil
}
