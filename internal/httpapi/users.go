package httpapi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (a *API) createUser(c *gin.Context) {
	w, r := c.Writer, c.Request
	var request *CreateUserRequest
	if !decode(w, r, &request) {
		return
	}
	if request == nil {
		a.fail(w, r, domain.ErrInvalid)
		return
	}
	user, err := a.service.CreateUser(r.Context(), request.ID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, user)
}

func (a *API) membership(c *gin.Context, add bool) {
	w, r := c.Writer, c.Request
	userID, err := pathID(c, "user_id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	segmentID, err := pathID(c, "segment_id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	err = a.service.SetMembership(r.Context(), userID, segmentID, add)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) addMembership(c *gin.Context) {
	a.membership(c, true)
}

func (a *API) removeMembership(c *gin.Context) {
	a.membership(c, false)
}

func (a *API) userSegments(c *gin.Context) {
	w, r := c.Writer, c.Request
	userID, err := pathID(c, "user_id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	segments, err := a.service.UserSegments(r.Context(), userID)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, segments)
}
