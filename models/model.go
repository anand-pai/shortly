package models

import "time"

// type CompanyUrl struct {
// 	CompanyId string    `db:"company_id"`
// 	Bucket    string    `db:"bucket"`
// 	CreatedAt time.Time `db:"created_at"`
// 	Slug      string    `db:"slug"`
// 	LongUrl   string    `db:"long_url"`
// }

// var companyUrlMeta = table.Metadata{
// 	Name:    "company_urls",
// 	Columns: []string{"company_id", "bucket", "created_at", "slug", "long_url"},
// 	PartKey: []string{"company_id", "bucket"},
// 	SortKey: []string{"created_at", "slug"},
// }

// type UrlMapping struct {
// 	Slug      string    `db:"slug"`
// 	Channel   int       `db:"channel"`
// 	CompanyId string    `db:"company_id"`
// 	CreatedAt time.Time `db:"created_at"`
// 	ExpiresAt time.Time `db:"expires_at"`
// 	LongUrl   string    `db:"long_url"`
// }

// var urlMappingMeta = table.Metadata{
// 	Name:    "url_mapping",
// 	Columns: []string{"slug", "channel", "company_id", "created_at", "expires_at", "long_url"},
// 	PartKey: []string{"slug"},
// 	SortKey: nil,
// }

// var CompanyUrlTable = table.New(companyUrlMeta)
// var UrlMappingTable = table.New(urlMappingMeta)

type ShortUrl struct {
	Slug      string    `json:"slug"`
	Channel   int       `json:"channel"`
	LongUrl   string    `json:"long_url" `
	CompanyId string    `json:"company_id" `
	Bucket    string    `json:"bucket"`
	Expiry    time.Time `json:"expiry"`
	Created   time.Time `json:"created"`
	IsAdvc    int       `json:"is_advc"`
}

type UrlMapping struct {
	Slug      string `json:"slug" db:"slug"`
	Channel   int    `json:"channel" db:"channel"`
	LongUrl   string `json:"long_url" db:"long_url"`
	CompanyId string `json:"company_id" db:"company_id"`
	Advanced  int    `json:"advanced" db:"advanced"`
}

type CompanyUrlCreated struct {
	CompanyId string    `json:"company_id" db:"company_id"`
	Bucket    string    `json:"bucket" db:"bucket"`
	Created   time.Time `json:"created" db:"created"`
	Slug      string    `json:"slug" db:"slug"`
}

type CompanyUrlExpiry struct {
	CompanyId string    `json:"company_id" db:"company_id"`
	Expiry    time.Time `json:"expiry" db:"expiry"`
	Slug      string    `json:"slug" db:"slug"`
}
