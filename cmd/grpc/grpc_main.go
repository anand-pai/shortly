package main

import (
	"context"
	"log"
	"shortly/helpers"
	"shortly/internal/generator"
	"shortly/internal/server"

	redisRepo "shortly/internal/redis"
	"shortly/internal/repository"
	"shortly/internal/scylla"
	"shortly/internal/services"

	grpcReg "shortly/gen/proto"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func init() {
	godotenv.Load()
}

func main() {
	loggerCfg := zap.NewProductionConfig()
	if helpers.GetEnv("LOG_LEVEL") == "debug" {
		loggerCfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}
	logger, err := loggerCfg.Build()
	if err != nil {
		log.Fatalf("build logger: %v", err)
	}
	defer logger.Sync()

	session := scylla.NewScyllaSession()
	defer session.Close()

	r := redisRepo.Redis{}
	redisClient := r.GetRedisConn()

	slugGen, err := generator.NewSlugGenerator(
		context.Background(),
		generator.NewRedisStateStore(redisClient.Client))

	urlShrtSvc := services.NewUrlShortnerService(session, logger, slugGen)

	register := func(s *grpc.Server) {
		grpcReg.RegisterUrlShortnerServiceServer(s, urlShrtSvc)
	}

	serverPort := helpers.GetEnv("GRPC_ADDRESS")
	metricsPort := helpers.GetEnv("METRICS_ADDRESS")
	httpAddress := helpers.GetEnv("HTTP_ADDRESS")

	if serverPort == "" {
		serverPort = ":8080"
	}
	if metricsPort == "" {
		metricsPort = ":9092"
	}
	if httpAddress == "" {
		httpAddress = ":8001"
	}

	if err := server.Run(
		serverPort,
		metricsPort,
		httpAddress,
		register,
		repository.NewScyllaTxtlyRepository(session), logger); err != nil {
		logger.Fatal("run grpc server", zap.Error(err))
	}
}
