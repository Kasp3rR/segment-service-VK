package httpapi

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (a *API) createSegment(c *gin.Context) {
	w, r := c.Writer, c.Request
	var request *CreateSegmentRequest
	if !decode(w, r, &request) {
		return
	}
	if request == nil {
		a.fail(w, r, domain.ErrInvalid)
		return
	}
	segment, err := a.service.CreateSegment(r.Context(), domain.CreateSegment{
		Code:        request.Code,
		Name:        request.Name,
		Description: request.Description,
	})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/segments/%d", segment.ID))
	writeJSON(w, http.StatusCreated, segment)
}

func (a *API) listSegments(c *gin.Context) {
	w, r := c.Writer, c.Request
	page := domain.Page{Limit: 100}
	var err error
	if value := r.URL.Query().Get("limit"); value != "" {
		page.Limit, err = strconv.Atoi(value)
		if err != nil {
			a.fail(w, r, domain.ErrInvalid)
			return
		}
	}
	if value := r.URL.Query().Get("after_id"); value != "" {
		page.AfterID, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			a.fail(w, r, domain.ErrInvalid)
			return
		}
	}
	segments, err := a.service.ListSegments(r.Context(), page)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, segments)
}

func (a *API) getSegment(c *gin.Context) {
	w, r := c.Writer, c.Request
	id, err := pathID(c, "id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	segment, err := a.service.GetSegment(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, segment)
}

func (a *API) patchSegment(c *gin.Context) {
	w, r := c.Writer, c.Request
	id, err := pathID(c, "id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	var request *UpdateSegmentRequest
	if !decode(w, r, &request) {
		return
	}
	if request == nil {
		a.fail(w, r, domain.ErrInvalid)
		return
	}
	segment, err := a.service.PatchSegment(r.Context(), id, domain.PatchSegment{
		Name:        request.Name.value,
		Description: request.Description.value,
	})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, segment)
}

func (a *API) deleteSegment(c *gin.Context) {
	w, r := c.Writer, c.Request
	id, err := pathID(c, "id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	err = a.service.DeleteSegment(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
