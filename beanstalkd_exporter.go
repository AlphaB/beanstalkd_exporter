package main

import (
	"flag"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kr/beanstalk"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"
)

const (
	namespace = "beanstalkd"
)

var (
	jobStatuses      = []string{"urgent", "ready", "reserved", "delayed", "buried"}
	rusages          = []string{"utime", "stime"}
	plainMetrics     = []string{"current-connections", "uptime", "binlog-records-written", "binlog-records-migrated"}
	tubeConns        = []string{"using", "watching", "waiting"}
	tubeCmds         = []string{"delete", "pause-tube"}
	tubePlainMetrics = []string{"pause", "total-jobs"}
)

var (
	listenAddress = flag.String("web.listen-address", ":9109", "Metrics and web interface address.")
	metricsPath   = flag.String("web.telemetry-path", "/metrics", "Path to metrics.")

	beanstalkdAddr  = flag.String("beanstalkd.addr", "localhost:11300", "Beanstalkd address")
	beanstalkdTubes = flag.String("beanstalkd.tubes", "", "Comma separated list of beanstalkd tubes to track. Leave empty to track all tubes")
)

// BeanstalkCollector is beanstalkd metrics collector that implements prometheus.Collector
type BeanstalkCollector struct {
	tubesMetrics     map[string]*prometheus.GaugeVec
	systemMetrics    map[string]*prometheus.GaugeVec
	conn             *beanstalk.Conn
	tubes            []string
	totalScrapes     prometheus.Counter
	duration, errors prometheus.Gauge
}

// check implemented interface
var _ prometheus.Collector = new(BeanstalkCollector)

// Describe implements the prometheus.Collector interface.
func (b *BeanstalkCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range b.tubesMetrics {
		metric.Describe(ch)
	}

	for _, metric := range b.systemMetrics {
		metric.Describe(ch)
	}

	b.totalScrapes.Describe(ch)
	b.duration.Describe(ch)
	b.errors.Describe(ch)
}

// Collect implements the prometheus.Collector interface.
func (b *BeanstalkCollector) Collect(ch chan<- prometheus.Metric) {
	ctrl := make(chan struct{})

	start := time.Now().UnixNano()
	go b.scrape(ctrl)
	<-ctrl

	b.duration.Set(float64(time.Now().UnixNano()-start) / 1000000000)

	for _, metric := range b.tubesMetrics {
		metric.Collect(ch)
	}

	for _, metric := range b.systemMetrics {
		metric.Collect(ch)
	}

	b.totalScrapes.Collect(ch)
	b.duration.Collect(ch)
	b.errors.Collect(ch)
}

func (b *BeanstalkCollector) scrape(ctrl chan<- struct{}) {
	var err error
	defer func() {
		if err != nil {
			log.Println("scrape error: ", err)
			b.errors.Set(1)
		}

		ctrl <- struct{}{}
	}()

	b.totalScrapes.Inc()

	stats, err := b.conn.Stats()
	if err != nil {
		return
	}

	var value int64
	for _, status := range jobStatuses {
		value, err = strconv.ParseInt(stats["current-jobs-"+status], 10, 64)
		if err != nil {
			return
		}
		b.systemMetrics["current-jobs"].WithLabelValues(status).Set(float64(value))
	}

	var flt float64
	for _, typ := range rusages {
		flt, err = strconv.ParseFloat(stats["rusage-"+typ], 10)
		if err != nil {
			return
		}
		b.systemMetrics["rusage"].WithLabelValues(typ).Set(float64(flt * 1000000))
	}

	for _, metric := range plainMetrics {
		value, err = strconv.ParseInt(stats[metric], 10, 64)
		if err != nil {
			return
		}
		b.systemMetrics[metric].WithLabelValues().Set(float64(value))
	}

	tubes, err := b.conn.ListTubes()
	if len(b.tubes) > 0 {
		var t []string
		for _, tube := range tubes {
			for _, selectedTube := range b.tubes {
				if tube == selectedTube {
					t = append(t, tube)
					break
				}
			}
		}
		tubes = t
	}

	for _, tube := range tubes {
		t := beanstalk.Tube{Conn: b.conn, Name: tube}
		stats, err = t.Stats()
		if err != nil {
			return
		}

		for _, status := range jobStatuses {
			value, err = strconv.ParseInt(stats["current-jobs-"+status], 10, 64)
			if err != nil {
				return
			}
			b.tubesMetrics["current-jobs"].WithLabelValues(tube, status).Set(float64(value))
		}

		for _, conn := range tubeConns {
			value, err = strconv.ParseInt(stats["current-"+conn], 10, 64)
			if err != nil {
				return
			}
			b.tubesMetrics["connections"].WithLabelValues(tube, conn).Set(float64(value))
		}

		for _, cmd := range tubeCmds {
			value, err = strconv.ParseInt(stats["cmd-"+cmd], 10, 64)
			if err != nil {
				return
			}
			b.tubesMetrics["cmd"].WithLabelValues(tube, cmd).Set(float64(value))
		}

		for _, metric := range tubePlainMetrics {
			value, err = strconv.ParseInt(stats[metric], 10, 64)
			if err != nil {
				return
			}
			b.tubesMetrics[metric].WithLabelValues(tube).Set(float64(value))
		}
	}
	b.errors.Set(0)
}

// NewBeanstalkdCollector establishes connection to given beanstalkd instance
// and returns initialized BeanstalkCollector
func NewBeanstalkdCollector(addr string, tubes []string) *BeanstalkCollector {
	conn, err := beanstalk.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	tubesMetrics := map[string]*prometheus.GaugeVec{
		"current-jobs": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tube_jobs_count",
			Help:      "Number of jobs in tube by statuses.",
		}, []string{"tube", "status"}),

		"connections": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tube_connections_count",
			Help:      "Number of open connection to tube by statuses.",
		}, []string{"tube", "status"}),

		"cmd": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tube_comands_total",
			Help:      "Total number of executed commands by tube.",
		}, []string{"tube", "command"}),

		"pause": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tube_pause_seconds_total",
			Help:      "The number of seconds the tube has been paused for.",
		}, []string{"tube"}),

		"total-jobs": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tube_total_jobs_count",
			Help:      "The cumulative count of jobs created in this tube in the current beanstalkd process.",
		}, []string{"tube"}),
	}

	systemMetrics := map[string]*prometheus.GaugeVec{
		"current-jobs": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "jobs_count",
			Help:      "Number of jobs in system by statuses.",
		}, []string{"status"}),

		"rusage": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cpu_usage_microseconds",
			Help:      "CPU time of this process.",
		}, []string{"type"}),

		"current-connections": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "connections_count",
			Help:      "The number of currently open connections.",
		}, []string{}),

		"uptime": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "uptime_seconds",
			Help:      "The number of seconds since this server process started running.",
		}, []string{}),

		"binlog-records-written": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "binlog_written_bytes",
			Help:      "The cumulative number of records written to the binlog.",
		}, []string{}),

		"binlog-records-migrated": prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "binlog_migrated_bytes",
			Help:      "The cumulative number of records written as part of compaction.",
		}, []string{}),
	}

	return &BeanstalkCollector{
		tubesMetrics:  tubesMetrics,
		systemMetrics: systemMetrics,
		conn:          conn,
		tubes:         tubes,
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrapes_total",
			Help:      "Current total beanstalkd scrapes.",
		}),
		duration: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "exporter_last_scrape_duration_seconds",
			Help:      "The last scrape duration.",
		}),

		errors: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "exporter_last_scrape_error",
			Help:      "The last scrape error status.",
		}),
	}
}

var indexPage = []byte(`<html>
<head><title>Beanstalkd exporter</title></head>
<body>
<h1>Beanstalkd exporter</h1>
<p><a href='` + *metricsPath + `'>Metrics</a></p>
</body>
</html>
`)

func main() {
	flag.Parse()

	var tubes []string
	if *beanstalkdTubes != "" {
		tubes = strings.Split(*beanstalkdTubes, ",")
	}

	exporter := NewBeanstalkdCollector(*beanstalkdAddr, tubes)
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write(indexPage)
	})

	log.Infof("Starting Server: %s", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
