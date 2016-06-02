package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/yosisa/webutil"
)

const namespace = "lmq"

var (
	listenAddress = flag.String("web.listen-address", ":9001", "Address on which to expose metrics.")
	metricsPath   = flag.String("web.metrics-path", "/metrics", "Path under which to expose metrics.")
	minInterval   = flag.Duration("collector.min-interval", 5*time.Second, "Minimum update interval.")
	lmqUri        = flag.String("lmq.uri", "http://localhost:9980/stats", "LMQ URI.")
)

type lmqStats struct {
	Queues map[string]*queueStats
}

type queueStats struct {
	Size   int
	Memory int
	Stats  struct {
		Push struct {
			Count int
		}
		Pull struct {
			Count int
		}
		Retention struct {
			Min    float64
			Max    float64
			Mean   float64 `json:"arithmetic_mean"`
			Median float64
		}
	}
}

type lmqCollector struct {
	uri             string
	interval        time.Duration
	expired         time.Time
	m               sync.Mutex
	size            *prometheus.GaugeVec
	memory          *prometheus.GaugeVec
	push            *prometheus.CounterVec
	pull            *prometheus.CounterVec
	retentionMin    *prometheus.GaugeVec
	retentionMax    *prometheus.GaugeVec
	retentionMean   *prometheus.GaugeVec
	retentionMedian *prometheus.GaugeVec
}

func newLMQCollector(uri string, interval time.Duration) *lmqCollector {
	labelNames := []string{"queue"}
	return &lmqCollector{
		uri:      uri,
		interval: interval,
		size: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "queue",
			Name:      "size",
			Help:      "Number of messages currently in the queue.",
		}, labelNames),
		memory: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "queue",
			Name:      "memory_bytes",
			Help:      "Used memory in bytes.",
		}, labelNames),
		push: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "queue",
			Name:      "push",
			Help:      "Number of messages pushed to the queue.",
		}, labelNames),
		pull: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "queue",
			Name:      "pull",
			Help:      "Number of messages pulled from the queue.",
		}, labelNames),
		retentionMin: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "queue",
			Name:      "retention_min",
			Help:      "The minimum retention time in seconds.",
		}, labelNames),
		retentionMax: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "queue",
			Name:      "retention_max",
			Help:      "The maximum retention time in seconds.",
		}, labelNames),
		retentionMean: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "queue",
			Name:      "retention_mean",
			Help:      "Mean time of retention times in seconds.",
		}, labelNames),
		retentionMedian: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "queue",
			Name:      "retention_median",
			Help:      "A median of retention times in seconds.",
		}, labelNames),
	}
}

func (c *lmqCollector) Describe(ch chan<- *prometheus.Desc) {
	c.size.Describe(ch)
	c.memory.Describe(ch)
	c.push.Describe(ch)
	c.pull.Describe(ch)
	c.retentionMin.Describe(ch)
	c.retentionMax.Describe(ch)
	c.retentionMean.Describe(ch)
	c.retentionMedian.Describe(ch)
}

func (c *lmqCollector) Collect(ch chan<- prometheus.Metric) {
	if err := c.updateStats(); err != nil {
		log.Printf("Failed to update metrics: %v", err)
		return
	}
	c.size.Collect(ch)
	c.memory.Collect(ch)
	c.push.Collect(ch)
	c.pull.Collect(ch)
	c.retentionMin.Collect(ch)
	c.retentionMax.Collect(ch)
	c.retentionMean.Collect(ch)
	c.retentionMedian.Collect(ch)
}

func (c *lmqCollector) updateStats() error {
	c.m.Lock()
	defer c.m.Unlock()
	if time.Now().Before(c.expired) {
		return nil
	}

	resp, err := http.Get(c.uri)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var stats lmqStats
	if err = json.Unmarshal(b, &stats); err != nil {
		return err
	}

	c.expired = time.Now().Add(c.interval)
	for name, q := range stats.Queues {
		c.size.WithLabelValues(name).Set(float64(q.Size))
		c.memory.WithLabelValues(name).Set(float64(q.Memory))
		c.push.WithLabelValues(name).Set(float64(q.Stats.Push.Count))
		c.pull.WithLabelValues(name).Set(float64(q.Stats.Pull.Count))
		c.retentionMin.WithLabelValues(name).Set(q.Stats.Retention.Min)
		c.retentionMax.WithLabelValues(name).Set(q.Stats.Retention.Max)
		c.retentionMean.WithLabelValues(name).Set(q.Stats.Retention.Mean)
		c.retentionMedian.WithLabelValues(name).Set(q.Stats.Retention.Median)
	}
	return nil
}

func main() {
	flag.Parse()
	c := newLMQCollector(*lmqUri, *minInterval)
	prometheus.MustRegister(c)
	http.Handle(*metricsPath, prometheus.Handler())
	h := webutil.Recoverer(http.DefaultServeMux, os.Stderr)

	log.Printf("Starting lmq_exporter at %s", *listenAddress)
	if err := http.ListenAndServe(*listenAddress, h); err != nil {
		log.Fatal(err)
	}
}
