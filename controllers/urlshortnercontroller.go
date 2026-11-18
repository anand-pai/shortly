package controllers

import (
	"encoding/json"
	"log"
	"net/http"
	"shortly/internal/generator"
	repo "shortly/internal/repository"
	"shortly/models"
)

type ShortnerController struct {
	SlugGen  *generator.SlugGenerator
	TxtlySvc repo.TxtlyRepository
}

func respond(w http.ResponseWriter, code int, data any) {
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(data)
}

func (s *ShortnerController) UrlShortnerCreate(w http.ResponseWriter, r *http.Request) {

	createRequest := models.ShortUrl{}

	if err := json.NewDecoder(r.Body).Decode(&createRequest); err != nil {
		log.Printf("unable body decode. err : %v", err)
		respond(w, http.StatusInternalServerError, map[string]any{
			"message": "request not processed",
			"error":   err.Error(),
		})
		return
	}

	slug, err := s.SlugGen.GenerateSlug(createRequest.Channel)

	if err != nil {
		log.Printf("unable gen slug : %v", err)
		respond(w, http.StatusInternalServerError, map[string]any{
			"message": "request not processed",
			"error":   err.Error(),
		})
		return
	}

	createRequest.Slug = slug

	data, err := s.TxtlySvc.CreateShortURL(r.Context(), createRequest)

	if err != nil {
		log.Printf("unable create short url : %v", err)
		respond(w, http.StatusInternalServerError, map[string]any{
			"message": "request not processed",
			"error":   err.Error(),
		})
		return
	}

	respond(w, http.StatusCreated, map[string]any{
		"message": "Txtly created",
		"data": map[string]any{
			"txtly": data,
		},
	})

}
