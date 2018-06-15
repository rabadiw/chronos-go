package chronos_test

import (
	"net/http"
	"net/url"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/ghttp"
	chronos "github.com/rabadiw/chronos-go"
)

func TestScheduler(t *testing.T) {
	g := NewGomegaWithT(t)

	server := ghttp.NewServer()
	defer server.Close()

	// test new client
	server.AppendHandlers(
		ghttp.CombineHandlers(
			ghttp.VerifyRequest("GET", "/v1/scheduler/jobs"),
		),
	)

	url, _ := url.Parse(server.URL())
	chronosStub := &chronos.Chronos{
		URL:            url,
		Debug:          false,
		RequestTimeout: 5,
		APIPrefix:      "v1",
	}

	g.Expect(chronosStub).To(BeAssignableToTypeOf(new(chronos.Chronos)))

	// test chronos failure
	server.Reset()
	server.AppendHandlers(
		ghttp.CombineHandlers(
			ghttp.VerifyRequest("GET", "/v1/scheduler/jobs"),
			ghttp.RespondWith(http.StatusInternalServerError, nil),
		),
	)

	_, err := chronosStub.Init()
	g.Expect(err).To(MatchError("Could not reach chronos cluster: 500 Internal Server Error"))

	//  test no API prefix handling
	server.AppendHandlers(
		ghttp.CombineHandlers(
			ghttp.VerifyRequest("GET", "/scheduler/jobs"),
		),
	)

	chronosStub = chronos.DefaultChronos()
	chronosStub.URL, _ = url.Parse(server.URL())

	g.Expect(chronosStub).To(BeAssignableToTypeOf(new(chronos.Chronos)))
}
