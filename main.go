package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"shortly/controllers"
	"shortly/generator"
	"shortly/helpers"
	repository "shortly/internal/redis"
	"shortly/internal/scylla"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type containers struct {
	UrlShorten *controllers.ShortnerController
}

func buildContainer(ctx context.Context) *containers {

	r := repository.Redis{}
	redisClient := r.GetRedisConn()
	slugGen, err := generator.NewSlugGenerator(ctx,
		generator.NewRedisStateStore(redisClient.Client))

	if err != nil {
		log.Panic("Error in slug gen instance init", err)
	}

	return &containers{
		UrlShorten: &controllers.ShortnerController{
			SlugGen: slugGen,
			Scylla:  scylla.NewScyllaSession(),
		},
	}

}

func init() {
	godotenv.Load()
}

func main() {

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	c := buildContainer(ctx)
	r := mux.NewRouter()

	r.Handle("/metrics", promhttp.Handler())
	r.HandleFunc("/shorten", c.UrlShorten.UrlShortnerCreate).Methods("POST")

	port := helpers.GetEnv("PORT")
	addr := fmt.Sprintf(":%s", port)

	server := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Error starting http server: %v", err)
		}
	}()

	log.Printf("Server started at : %s", port)

	<-ctx.Done()

	log.Println("Shutdown signal received")

	// _, shutdownCancel := context.WithTimeout(
	// 	context.Background(),
	// 	5*time.Second,
	// )
	// defer shutdownCancel()

	// if err := server.Shutdown(shutdownCtx); err != nil {
	// 	log.Printf("Server shutdown error: %v", err)
	// }

	log.Println("Server stopped")
}
