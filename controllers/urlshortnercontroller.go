package controllers

import (
	"encoding/json"
	"net/http"
	"shortly/generator"

	"github.com/scylladb/gocqlx/v3"
)

type ShortnerController struct {
	SlugGen *generator.SlugGenerator
	Scylla  gocqlx.Session
}

func (s *ShortnerController) UrlShortnerCreate(w http.ResponseWriter, r *http.Request) {

	slug, err := s.SlugGen.GenerateSlug()

	if err != nil {
		w.Header().Add("content-type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": err.Error(),
		})
		return
	}

	w.Header().Add("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "Server healthy",
		"slug":   slug,
	})

}
