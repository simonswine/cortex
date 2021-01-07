// +build requires_docker

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/prometheus/alertmanager/notify/webhook"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/ruler"
)

type message struct {
	webhook    *webhook.Message
	remoteAddr string
	received   time.Time
}

func handler(t *testing.T, msgCh chan message) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var webhook webhook.Message
		if err := json.NewDecoder(r.Body).Decode(&webhook); err != nil {
			t.Errorf("failed to parse json body: %v", err)
		}
		t.Logf("received request: %s %+v", r.RemoteAddr, webhook.Data)

		msgCh <- message{
			webhook:    &webhook,
			remoteAddr: r.RemoteAddr,
			received:   time.Now(),
		}

		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}
}

func TestAlertmanagerCluster(t *testing.T) {
	upstream := false

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	myNetworkIP, err := exec.Command(
		"docker", "network", "inspect",
		networkName,
		"--format", "{{range $net := .IPAM.Config}}{{$net.Gateway}}{{end}}",
	).Output()

	listener, err := net.Listen("tcp", strings.TrimSpace(string(myNetworkIP))+":0")
	require.NoError(t, err)

	msgCh := make(chan message, 128)
	messagesByGroupKey := make(map[string][]message)

	go func() {
		for msg := range msgCh {
			if _, ok := messagesByGroupKey[msg.webhook.GroupKey]; !ok {
				messagesByGroupKey[msg.webhook.GroupKey] = []message{
					msg,
				}
			} else {
				messagesByGroupKey[msg.webhook.GroupKey] = append(
					messagesByGroupKey[msg.webhook.GroupKey],
					msg,
				)
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/", handler(t, msgCh))
	srv := &http.Server{}
	srv.Handler = mux
	go func() {
		err := srv.Serve(listener)
		if err != nil {
			require.NoError(t, err)
		}
	}()
	defer srv.Close()

	srvURL := "http://" + listener.Addr().String()

	t.Logf("server_addr = %s", srvURL)

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs/user-1.yaml", []byte(fmt.Sprintf(`---
global:
  resolve_timeout: 30s
route:
  receiver: 'default-receiver'
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 4h
  group_by: [cluster, alertname]
receivers:
  - name: 'default-receiver'
    webhook_configs:
      - url: %s
`, srvURL))))

	alertmanagers := make([]*e2ecortex.CortexService, 3)

	image := e2ecortex.GetDefaultImage()
	if upstream {
		image = "quay.io/prometheus/alertmanager:v0.21.0"
	}

	containerName := func(pos int) string {
		return fmt.Sprintf("%s-alertmanager-%d", networkName, pos)
	}

	clusterFlags := make([]string, len(alertmanagers))
	for pos := range clusterFlags {
		clusterFlags[pos] = fmt.Sprintf("--cluster.peer=%s:9094", containerName(pos))
	}

	for pos := range alertmanagers {
		cmd := "cortex"
		flags := e2e.BuildArgs(e2e.MergeFlags(
			map[string]string{
				"-target":                               "alertmanager",
				"-log.level":                            "warn",
				"-experimental.alertmanager.enable-api": "true",
			},
			AlertmanagerFlags(),
			AlertmanagerLocalFlags(),
		))
		httpPort := 80
		grpcPort := 9093
		readyPath := "/ready"

		if upstream {
			cmd = "alertmanager"
			flags = []string{
				"--config.file", filepath.Join(e2e.ContainerSharedDir, "alertmanager_configs/user-1.yaml"),
				//"--cluster.pushpull-interval", "15s",
				//"--cluster.gossip-interval", "5ms",
			}
			readyPath = "/-" + readyPath
			httpPort = 9093
			grpcPort = 9099
		}

		flags = append(flags, clusterFlags...)

		alertmanagers[pos] = e2ecortex.NewCortexService(
			fmt.Sprintf("alertmanager-%d", pos),
			image,
			e2e.NewCommandWithoutEntrypoint(cmd, flags...),
			e2e.NewHTTPReadinessProbe(httpPort, readyPath, 200, 299),
			httpPort,
			grpcPort,
		)
		alertmanagers[pos].ConcreteService.SetUser("root")

		require.NoError(t, s.Start(alertmanagers[pos]))
	}

	// set some latency on all container's devices
	/*
		for pos := range alertmanagers {
			cmd := exec.Command(
				"docker",
				"run",
				"--rm",
				"--cap-add=NET_ADMIN",
				fmt.Sprintf("--network=container:%s", containerName(pos)),
				"alpine:3.12",
				"sh", "-xeuc",
				"apk add --update iproute2 && tc qdisc add dev eth0 root netem delay 100ms",
			)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			require.NoError(t, cmd.Run())
		}
	*/

	// wait for alertmanagers to be ready
	for pos := range alertmanagers {
		require.NoError(t, s.WaitReady(alertmanagers[pos]))
		if !upstream {
			require.NoError(t, alertmanagers[pos].WaitSumMetrics(e2e.Equals(1), "cortex_alertmanager_config_last_reload_successful"))
			require.NoError(t, alertmanagers[pos].WaitSumMetrics(e2e.Greater(0), "cortex_alertmanager_config_hash"))
		}
	}

	// send alerts continously
	start := time.Now()
	ticker := time.NewTicker(5 * time.Second)
	count := 0
	for {
		count++
		for pos := range alertmanagers {

			alert := []*ruler.Alert{{
				State: "firing",
				Labels: []labels.Label{
					{
						Name:  "cluster",
						Value: fmt.Sprintf("my-cluster-%d", count),
					},
				},
			}}

			b := new(bytes.Buffer)
			json.NewEncoder(b).Encode(&alert)

			url := "http://%s/api/prom/api/v1/alerts"
			if upstream {
				url = "http://%s/api/v1/alerts"
			}

			req, err := http.NewRequest("POST", fmt.Sprintf(url, alertmanagers[pos].HTTPEndpoint()), b)
			assert.NoError(t, err)
			req.Header.Set("X-Scope-OrgID", "user-1")

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := http.DefaultClient.Do(req.WithContext(ctx))
			assert.NoError(t, err)
			defer res.Body.Close()
		}
		if count >= 6 {
			break
		}

		<-ticker.C

	}

	// wait for all webhooks to arrive 30 secs
	time.Sleep(120 * time.Second)

	groupKeys := make([]string, 0, len(messagesByGroupKey))
	for key, _ := range messagesByGroupKey {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprintln(w, "Alertmanger\tGroup Key\tTime\tStatus")

	for _, key := range groupKeys {
		msgs := messagesByGroupKey[key]

		for _, msg := range msgs {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", msg.remoteAddr, msg.webhook.GroupKey, msg.received.Sub(start), msg.webhook.Status)
		}
	}
	w.Flush()

	time.Sleep(time.Hour)
}

func TestAlertmanager(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs/user-1.yaml", []byte(cortexAlertmanagerUserConfigYaml)))

	alertmanager := e2ecortex.NewAlertmanager(
		"alertmanager",
		mergeFlags(
			AlertmanagerFlags(),
			AlertmanagerLocalFlags(),
		),
		"",
	)
	require.NoError(t, s.StartAndWaitReady(alertmanager))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Equals(1), "cortex_alertmanager_config_last_reload_successful"))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Greater(0), "cortex_alertmanager_config_hash"))

	c, err := e2ecortex.NewClient("", "", alertmanager.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	cfg, err := c.GetAlertmanagerConfig(context.Background())
	require.NoError(t, err)

	// Ensure the returned status config matches alertmanager_test_fixtures/user-1.yaml
	require.NotNil(t, cfg)
	require.Equal(t, "example_receiver", cfg.Route.Receiver)
	require.Len(t, cfg.Route.GroupByStr, 1)
	require.Equal(t, "example_groupby", cfg.Route.GroupByStr[0])
	require.Len(t, cfg.Receivers, 1)
	require.Equal(t, "example_receiver", cfg.Receivers[0].Name)

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, AlertManager, alertmanager)

	// Test compression by inspecting the response Headers
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/api/v1/alerts", alertmanager.HTTPEndpoint()), nil)
	require.NoError(t, err)

	req.Header.Set("X-Scope-OrgID", "user-1")
	req.Header.Set("Accept-Encoding", "gzip")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Execute HTTP request
	res, err := http.DefaultClient.Do(req.WithContext(ctx))
	require.NoError(t, err)

	defer res.Body.Close()
	// We assert on the Vary header as the minimum response size for enabling compression is 1500 bytes.
	// This is enough to know whenever the handler for compression is enabled or not.
	require.Equal(t, "Accept-Encoding", res.Header.Get("Vary"))
}

func TestAlertmanagerStoreAPI(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(AlertmanagerFlags(), AlertmanagerS3Flags())

	minio := e2edb.NewMinio(9000, flags["-alertmanager.storage.s3.buckets"])
	require.NoError(t, s.StartAndWaitReady(minio))

	am := e2ecortex.NewAlertmanager(
		"alertmanager",
		flags,
		"",
	)

	require.NoError(t, s.StartAndWaitReady(am))
	require.NoError(t, am.WaitSumMetrics(e2e.Equals(1), "alertmanager_cluster_members"))

	c, err := e2ecortex.NewClient("", "", am.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	_, err = c.GetAlertmanagerConfig(context.Background())
	require.Error(t, err)
	require.EqualError(t, err, e2ecortex.ErrNotFound.Error())

	err = c.SetAlertmanagerConfig(context.Background(), cortexAlertmanagerUserConfigYaml, map[string]string{})
	require.NoError(t, err)

	require.NoError(t, am.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_alertmanager_config_last_reload_successful"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1")),
		e2e.WaitMissingMetrics))
	require.NoError(t, am.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_alertmanager_config_last_reload_successful_seconds"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1")),
		e2e.WaitMissingMetrics))

	cfg, err := c.GetAlertmanagerConfig(context.Background())
	require.NoError(t, err)

	// Ensure the returned status config matches the loaded config
	require.NotNil(t, cfg)
	require.Equal(t, "example_receiver", cfg.Route.Receiver)
	require.Len(t, cfg.Route.GroupByStr, 1)
	require.Equal(t, "example_groupby", cfg.Route.GroupByStr[0])
	require.Len(t, cfg.Receivers, 1)
	require.Equal(t, "example_receiver", cfg.Receivers[0].Name)

	err = c.SendAlertToAlermanager(context.Background(), &model.Alert{Labels: model.LabelSet{"foo": "bar"}})
	require.NoError(t, err)

	require.NoError(t, am.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_alertmanager_alerts_received_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1")),
		e2e.WaitMissingMetrics))

	err = c.DeleteAlertmanagerConfig(context.Background())
	require.NoError(t, err)

	// The deleted config is applied asynchronously, so we should wait until the metric
	// disappear for the specific user.
	require.NoError(t, am.WaitRemovedMetric("cortex_alertmanager_config_last_reload_successful", e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))
	require.NoError(t, am.WaitRemovedMetric("cortex_alertmanager_config_last_reload_successful_seconds", e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))

	cfg, err = c.GetAlertmanagerConfig(context.Background())
	require.Error(t, err)
	require.Nil(t, cfg)
	require.EqualError(t, err, "not found")
}
