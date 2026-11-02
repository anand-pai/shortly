package services

import (
	"context"
	"fmt"
	gen "shortly/gen/proto"
	"shortly/helpers"
	"shortly/internal/generator"
	"shortly/internal/repository"
	"shortly/internal/server"
	"shortly/models"

	grpcGen "shortly/gen/proto"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UrlShortnerService struct {
	grpcGen.UnimplementedUrlShortnerServiceServer
	session   *gocql.Session
	txtlyRepo repository.TxtlyRepository
	log       *zap.Logger
	SlugGen   *generator.SlugGenerator
}

func NewUrlShortnerService(session *gocql.Session, zapLog *zap.Logger, slugGen *generator.SlugGenerator) *UrlShortnerService {
	return &UrlShortnerService{
		session:   session,
		txtlyRepo: repository.NewScyllaTxtlyRepository(session),
		log:       zapLog,
		SlugGen:   slugGen,
	}
}

func (s *UrlShortnerService) requestLogger(ctx context.Context, method string) *zap.Logger {
	return s.log.With(
		zap.String("request_id", server.RequestIDFromContext(ctx)),
		zap.String("method", method),
	)
}

func (s *UrlShortnerService) CreateShortUrl(ctx context.Context, req *gen.CreateUrlRequest) (*gen.CreateUrlResponse, error) {
	logger := s.requestLogger(ctx, "CreateShortUrl")

	shortUrlPayload := models.ShortUrl{
		LongUrl:   req.LongUrl,
		Channel:   int(req.Channel),
		IsAdvc:    int(req.IsAdvc),
		CompanyId: req.CompanyId,
	}

	if req.Slug != "" {
		shortUrlPayload.Slug = req.Slug
	} else {
		value, err := s.SlugGen.GenerateSlug()
		if err != nil {
			logger.Error("failed to create short url", zap.Error(err))
			return nil, status.Errorf(codes.Internal, "failed to generate slug")
		}
		shortUrlPayload.Slug = value
	}

	shortUrl, err := s.txtlyRepo.CreateShortURL(ctx, shortUrlPayload)
	if err != nil {
		logger.Error("failed to create short url", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create short url")
	}

	return &gen.CreateUrlResponse{
		ShortUrl: fmt.Sprintf("https://%s/%s", helpers.GetEnv("SHORTLY_DOMAIN"), shortUrl.Slug),
	}, nil
}
