package httpapi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (a *API) startDistribution(c *gin.Context) {
	w, r := c.Writer, c.Request
	id, err := pathID(c, "id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	var request *CreateDistributionRequest
	if !decode(w, r, &request) {
		return
	}
	if request == nil {
		a.fail(w, r, domain.ErrInvalid)
		return
	}
	job, err := a.service.StartDistribution(r.Context(), id, string(request.Percent))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	w.Header().Set("Location", "/distributions/"+job.ID)
	writeJSON(w, http.StatusAccepted, job)
}

func (a *API) getDistribution(c *gin.Context) {
	job, err := a.service.GetDistribution(c.Request.Context(), c.Param("job_id"))
	if err != nil {
		a.fail(c.Writer, c.Request, err)
		return
	}
	writeJSON(c.Writer, http.StatusOK, job)
}
