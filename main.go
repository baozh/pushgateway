// Copyright 2014 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	gom "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"

	dto "github.com/prometheus/client_model/go"
	promlogflag "github.com/prometheus/common/promlog/flag"

	"github.com/prometheus/common/expfmt"
	api_v1 "github.com/prometheus/pushgateway/api/v1"
	"github.com/prometheus/pushgateway/asset"
	"github.com/prometheus/pushgateway/handler"
	"github.com/prometheus/pushgateway/storage"
)

func init() {
	prometheus.MustRegister(version.NewCollector("pushgateway"))
}

// logFunc in an adaptor to plug gokit logging into promhttp.HandlerOpts.
type logFunc func(...interface{}) error

func (lf logFunc) Println(v ...interface{}) {
	lf("msg", fmt.Sprintln(v...))
}

func main() {
	var (
		app = kingpin.New(filepath.Base(os.Args[0]), "The Pushgateway")

		listenAddress       = app.Flag("web.listen-address", "Address to listen on for the web interface, API, and telemetry.").Default(":9091").String()
		metricsPath         = app.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		externalURL         = app.Flag("web.external-url", "The URL under which the Pushgateway is externally reachable.").Default("").URL()
		routePrefix         = app.Flag("web.route-prefix", "Prefix for the internal routes of web endpoints. Defaults to the path of --web.external-url.").Default("").String()
		enableLifeCycle     = app.Flag("web.enable-lifecycle", "Enable shutdown via HTTP request.").Default("false").Bool()
		enableAdminAPI      = app.Flag("web.enable-admin-api", "Enable API endpoints for admin control actions.").Default("false").Bool()
		persistenceFile     = app.Flag("persistence.file", "File to persist metrics. If empty, metrics are only kept in memory.").Default("").String()
		persistenceInterval = app.Flag("persistence.interval", "The minimum interval at which to write out the persistence file.").Default("5m").Duration()
		pushUnchecked       = app.Flag("push.disable-consistency-check", "Do not check consistency of pushed metrics. DANGEROUS.").Default("false").Bool()
		keepLastSec         = app.Flag("keep.last.sec", "keep last second metric date").Default("60").Int()
		processNum          = app.Flag("process.num", "process num").Default("24").Int()

		promlogConfig = promlog.Config{}
	)
	promlogflag.AddFlags(app, &promlogConfig)
	app.Version(version.Print("pushgateway"))
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))
	logger := promlog.New(&promlogConfig)

	*routePrefix = computeRoutePrefix(*routePrefix, *externalURL)
	externalPathPrefix := computeRoutePrefix("", *externalURL)

	level.Info(logger).Log("msg", "starting pushgateway", "version", version.Info())
	level.Info(logger).Log("build_context", version.BuildContext())
	level.Debug(logger).Log("msg", "external URL", "url", *externalURL)
	level.Debug(logger).Log("msg", "path prefix used externally", "path", externalPathPrefix)
	level.Debug(logger).Log("msg", "path prefix for internal routing", "path", *routePrefix)

	// flags is used to show command line flags on the status page.
	// Kingpin default flags are excluded as they would be confusing.
	flags := map[string]string{}
	boilerplateFlags := kingpin.New("", "").Version("")
	for _, f := range app.Model().Flags {
		if boilerplateFlags.GetFlag(f.Name) == nil {
			flags[f.Name] = f.Value.String()
		}
	}

	ms := storage.NewDiskMetricStore(*persistenceFile, *persistenceInterval, prometheus.DefaultGatherer, logger, *processNum)

	// Create a Gatherer combining the DefaultGatherer and the metrics from the metric store.
	g := prometheus.Gatherers{
		prometheus.DefaultGatherer,
		prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { return ms.GetMetricFamilies(*keepLastSec), nil }),
	}

	r := route.New()
	r.Get(*routePrefix+"/-/healthy", handler.Healthy(ms).ServeHTTP)
	r.Get(*routePrefix+"/-/chan-len", handler.ChanLen(ms).ServeHTTP)
	r.Get(*routePrefix+"/-/ready", handler.Ready(ms).ServeHTTP)
	r.Get(
		path.Join(*routePrefix, *metricsPath),
		HandlerFor(ms, true, g, promhttp.HandlerOpts{
			ErrorLog: logFunc(level.Error(logger).Log),
		}).ServeHTTP,
	)

	gO := prometheus.Gatherers{
		prometheus.DefaultGatherer,
		prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { return ms.GetMetricFamilies(0), nil }),
	}
	r.Get(
		*routePrefix+"/metrics-all",
		HandlerFor(ms, false, gO, promhttp.HandlerOpts{
			ErrorLog: logFunc(level.Error(logger).Log),
		}).ServeHTTP,
	)

	// Handlers for pushing and deleting metrics.
	pushAPIPath := *routePrefix + "/metrics"
	for _, suffix := range []string{"", handler.Base64Suffix} {
		jobBase64Encoded := suffix == handler.Base64Suffix
		r.Put(pushAPIPath+"/job"+suffix+"/:job/*labels", handler.Push(ms, true, !*pushUnchecked, jobBase64Encoded, logger))
		r.Post(pushAPIPath+"/job"+suffix+"/:job/*labels", handler.Push(ms, false, !*pushUnchecked, jobBase64Encoded, logger))
		r.Del(pushAPIPath+"/job"+suffix+"/:job/*labels", handler.Delete(ms, jobBase64Encoded, logger))
		r.Put(pushAPIPath+"/job"+suffix+"/:job", handler.Push(ms, true, !*pushUnchecked, jobBase64Encoded, logger))
		r.Post(pushAPIPath+"/job"+suffix+"/:job", handler.Push(ms, false, !*pushUnchecked, jobBase64Encoded, logger))
		r.Del(pushAPIPath+"/job"+suffix+"/:job", handler.Delete(ms, jobBase64Encoded, logger))
	}
	r.Get(*routePrefix+"/static/*filepath", handler.Static(asset.Assets, *routePrefix).ServeHTTP)

	statusHandler := handler.Status(ms, asset.Assets, flags, externalPathPrefix, logger)
	r.Get(*routePrefix+"/status", statusHandler.ServeHTTP)
	r.Get(*routePrefix+"/", statusHandler.ServeHTTP)

	// Re-enable pprof.
	r.Get(*routePrefix+"/debug/pprof/*pprof", handlePprof)

	level.Info(logger).Log("listen_address", *listenAddress)
	l, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	quitCh := make(chan struct{})
	quitHandler := func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Requesting termination... Goodbye!")
		close(quitCh)
	}

	forbiddenAPINotEnabled := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("Lifecycle API is not enabled."))
	}

	if *enableLifeCycle {
		r.Put(*routePrefix+"/-/quit", quitHandler)
		r.Post(*routePrefix+"/-/quit", quitHandler)
	} else {
		r.Put(*routePrefix+"/-/quit", forbiddenAPINotEnabled)
		r.Post(*routePrefix+"/-/quit", forbiddenAPINotEnabled)
	}

	r.Get("/-/quit", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("Only POST or PUT requests allowed."))
	})

	mux := http.NewServeMux()
	mux.Handle("/", r)

	buildInfo := map[string]string{
		"version":   version.Version,
		"revision":  version.Revision,
		"branch":    version.Branch,
		"buildUser": version.BuildUser,
		"buildDate": version.BuildDate,
		"goVersion": version.GoVersion,
	}

	apiv1 := api_v1.New(logger, ms, flags, buildInfo)

	apiPath := "/api"
	if *routePrefix != "/" {
		apiPath = *routePrefix + apiPath
	}

	av1 := route.New()
	apiv1.Register(av1)
	apiv1.SetKeepLastSec(*keepLastSec)
	if *enableAdminAPI {
		av1.Put("/admin/wipe", handler.WipeMetricStore(ms, logger).ServeHTTP)
	}

	mux.Handle(apiPath+"/v1/", http.StripPrefix(apiPath+"/v1", av1))

	go closeListenerOnQuit(l, quitCh, logger)
	err = (&http.Server{Addr: *listenAddress, Handler: mux}).Serve(l)
	level.Error(logger).Log("msg", "HTTP server stopped", "err", err)
	// To give running connections a chance to submit their payload, we wait
	// for 1sec, but we don't want to wait long (e.g. until all connections
	// are done) to not delay the shutdown.
	time.Sleep(time.Second)
	if err := ms.Shutdown(); err != nil {
		level.Error(logger).Log("msg", "problem shutting down metric storage", "err", err)
	}
}

func handlePprof(w http.ResponseWriter, r *http.Request) {
	switch route.Param(r.Context(), "pprof") {
	case "/cmdline":
		pprof.Cmdline(w, r)
	case "/profile":
		pprof.Profile(w, r)
	case "/symbol":
		pprof.Symbol(w, r)
	default:
		pprof.Index(w, r)
	}
}

// computeRoutePrefix returns the effective route prefix based on the
// provided flag values for --web.route-prefix and
// --web.external-url. With prefix empty, the path of externalURL is
// used instead. A prefix "/" results in an empty returned prefix. Any
// non-empty prefix is normalized to start, but not to end, with "/".
func computeRoutePrefix(prefix string, externalURL *url.URL) string {
	if prefix == "" {
		prefix = externalURL.Path
	}

	if prefix == "/" {
		prefix = ""
	}

	if prefix != "" {
		prefix = "/" + strings.Trim(prefix, "/")
	}

	return prefix
}

// closeListenerOnQuite closes the provided listener upon closing the provided
// quitCh or upon receiving a SIGINT or SIGTERM.
func closeListenerOnQuit(l net.Listener, quitCh <-chan struct{}, logger log.Logger) {
	notifier := make(chan os.Signal, 1)
	signal.Notify(notifier, os.Interrupt, syscall.SIGTERM)

	select {
	case <-notifier:
		level.Info(logger).Log("msg", "received SIGINT/SIGTERM; exiting gracefully...")
		break
	case <-quitCh:
		level.Warn(logger).Log("msg", "received termination request via web service, exiting gracefully...")
		break
	}
	l.Close()
}

func HandlerFor(ms *storage.DiskMetricStore, needClear bool, reg prometheus.Gatherer, opts promhttp.HandlerOpts) http.Handler {
	var (
		inFlightSem chan struct{}
		errCnt      = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "promhttp_metric_handler_errors_total",
				Help: "Total number of internal errors encountered by the promhttp metric handler.",
			},
			[]string{"cause"},
		)
	)

	if opts.MaxRequestsInFlight > 0 {
		inFlightSem = make(chan struct{}, opts.MaxRequestsInFlight)
	}
	if opts.Registry != nil {
		// Initialize all possibilites that can occur below.
		errCnt.WithLabelValues("gathering")
		errCnt.WithLabelValues("encoding")
		if err := opts.Registry.Register(errCnt); err != nil {
			if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
				errCnt = are.ExistingCollector.(*prometheus.CounterVec)
			} else {
				panic(err)
			}
		}
	}

	h := http.HandlerFunc(func(rsp http.ResponseWriter, req *http.Request) {
		defer func() {
			if needClear {
				ms.ClearStore()
			}
		}()

		fmt.Printf("ts:%s, proxy_metric: recv metrics request\n", time.Now().Format("2006-01-02 15:04:05"))
		if inFlightSem != nil {
			select {
			case inFlightSem <- struct{}{}: // All good, carry on.
				defer func() { <-inFlightSem }()
			default:
				http.Error(rsp, fmt.Sprintf(
					"Limit of concurrent requests reached (%d), try again later.", opts.MaxRequestsInFlight,
				), http.StatusServiceUnavailable)
				return
			}
		}
		mfs, err := reg.Gather()
		if err != nil {
			if opts.ErrorLog != nil {
				opts.ErrorLog.Println("error gathering metrics:", err)
			}
			errCnt.WithLabelValues("gathering").Inc()
			switch opts.ErrorHandling {
			case promhttp.PanicOnError:
				panic(err)
			case promhttp.ContinueOnError:
				if len(mfs) == 0 {
					// Still report the error if no metrics have been gathered.
					httpError(rsp, err)
					return
				}
			case promhttp.HTTPErrorOnError:
				httpError(rsp, err)
				return
			}
		}

		var contentType expfmt.Format
		if opts.EnableOpenMetrics {
			contentType = expfmt.NegotiateIncludingOpenMetrics(req.Header)
		} else {
			contentType = expfmt.Negotiate(req.Header)
		}
		header := rsp.Header()
		header.Set(contentTypeHeader, string(contentType))

		w := io.Writer(rsp)
		if !opts.DisableCompression && gzipAccepted(req.Header) {
			header.Set(contentEncodingHeader, "gzip")
			gz := gzipPool.Get().(*gzip.Writer)
			defer gzipPool.Put(gz)

			gz.Reset(w)
			defer gz.Close()

			w = gz
		}

		enc := expfmt.NewEncoder(w, contentType)

		var lastErr error

		// handleError handles the error according to opts.ErrorHandling
		// and returns true if we have to abort after the handling.
		handleError := func(err error) bool {
			if err == nil {
				return false
			}
			lastErr = err
			if opts.ErrorLog != nil {
				opts.ErrorLog.Println("error encoding and sending metric family:", err)
			}
			errCnt.WithLabelValues("encoding").Inc()
			switch opts.ErrorHandling {
			case promhttp.PanicOnError:
				panic(err)
			case promhttp.HTTPErrorOnError:
				httpError(rsp, err)
				return true
			}
			// Do nothing in all other cases, including ContinueOnError.
			return false
		}

		for _, mf := range mfs {
			job := ""
			if len(mf.Metric) > 0 {
				job = GetJobValue(mf.Metric[0].Label)
			}
			handler.ProxyMetricCnt.With(map[string]string{"metric_name": *mf.Name, "job": job}).Add(float64(len(mf.Metric)))
			fmt.Printf("ts:%s, proxy_metric: job:%s, metric_name:%s, metrics_len:%d\n",
				time.Now().Format("2006-01-02 15:04:05"), job, *mf.Name, len(mf.Metric))
			if handleError(enc.Encode(mf)) {
				return
			}
		}
		if closer, ok := enc.(expfmt.Closer); ok {
			// This in particular takes care of the final "# EOF\n" line for OpenMetrics.
			if handleError(closer.Close()) {
				return
			}
		}

		if lastErr != nil {
			httpError(rsp, lastErr)
		}
	})

	if opts.Timeout <= 0 {
		return h
	}
	return http.TimeoutHandler(h, opts.Timeout, fmt.Sprintf(
		"Exceeded configured timeout of %v.\n",
		opts.Timeout,
	))
}

//const (
//	// Serve an HTTP status code 500 upon the first error
//	// encountered. Report the error message in the body.
//	HTTPErrorOnError HandlerErrorHandling = iota
//	// Ignore errors and try to serve as many metrics as possible.  However,
//	// if no metrics can be served, serve an HTTP status code 500 and the
//	// last error message in the body. Only use this in deliberate "best
//	// effort" metrics collection scenarios. In this case, it is highly
//	// recommended to provide other means of detecting errors: By setting an
//	// ErrorLog in HandlerOpts, the errors are logged. By providing a
//	// Registry in HandlerOpts, the exposed metrics include an error counter
//	// "promhttp_metric_handler_errors_total", which can be used for
//	// alerts.
//	ContinueOnError
//	// Panic upon the first error encountered (useful for "crash only" apps).
//	PanicOnError
//)

func httpError(rsp http.ResponseWriter, err error) {
	rsp.Header().Del(contentEncodingHeader)
	http.Error(
		rsp,
		"An error has occurred while serving metrics:\n\n"+err.Error(),
		http.StatusInternalServerError,
	)
}

const (
	contentTypeHeader     = "Content-Type"
	contentEncodingHeader = "Content-Encoding"
	acceptEncodingHeader  = "Accept-Encoding"
)

func gzipAccepted(header http.Header) bool {
	a := header.Get(acceptEncodingHeader)
	parts := strings.Split(a, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "gzip" || strings.HasPrefix(part, "gzip;") {
			return true
		}
	}
	return false
}

var gzipPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}

type HandlerErrorHandling int

func GetJobValue(lps []*gom.LabelPair) string {
	if len(lps) == 0 {
		return ""
	}

	for _, lb := range lps {
		if lb == nil {
			continue
		}
		if *(*lb).Name == "job" {
			return *(*lb).Value
		}
	}
	return ""
}
