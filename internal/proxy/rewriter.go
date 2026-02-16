// Package proxy provides URL rewriting and reverse proxy functionality for apt-proxy.
// It handles distribution-specific URL patterns and routes requests to configured mirrors.
package proxy

import (
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"

	logger "github.com/soulteary/logger-kit"

	"github.com/soulteary/apt-proxy/internal/benchmarks"
	"github.com/soulteary/apt-proxy/internal/distro"
	"github.com/soulteary/apt-proxy/internal/mirrors"
	"github.com/soulteary/apt-proxy/internal/state"
)

// URLRewriter holds the mirror and pattern for URL rewriting
type URLRewriter struct {
	mirror  *url.URL
	pattern *regexp.Regexp
}

// URLRewriters manages rewriters for different distributions
type URLRewriters struct {
	// Dynamic map for all distro types (including YAML-loaded ones)
	byType map[int]*URLRewriter
	Mu     sync.RWMutex
}

// builtinDistroModes lists the built-in distro types for fallback
var builtinDistroModes = []int{
	distro.TYPE_LINUX_DISTROS_UBUNTU,
	distro.TYPE_LINUX_DISTROS_UBUNTU_PORTS,
	distro.TYPE_LINUX_DISTROS_DEBIAN,
	distro.TYPE_LINUX_DISTROS_CENTOS,
	distro.TYPE_LINUX_DISTROS_ALPINE,
}

var builtinModeRules = map[int][]distro.Rule{
	distro.TYPE_LINUX_DISTROS_UBUNTU:       distro.UBUNTU_DEFAULT_CACHE_RULES,
	distro.TYPE_LINUX_DISTROS_UBUNTU_PORTS: distro.UBUNTU_PORTS_DEFAULT_CACHE_RULES,
	distro.TYPE_LINUX_DISTROS_DEBIAN:       distro.DEBIAN_DEFAULT_CACHE_RULES,
	distro.TYPE_LINUX_DISTROS_CENTOS:       distro.CENTOS_DEFAULT_CACHE_RULES,
	distro.TYPE_LINUX_DISTROS_ALPINE:       distro.ALPINE_DEFAULT_CACHE_RULES,
}

// builtinRewriterConfig maps built-in distro types to their state getter and name
var builtinRewriterConfig = map[int]struct {
	getMirror func() *url.URL
	name      string
}{
	distro.TYPE_LINUX_DISTROS_UBUNTU:       {state.GetUbuntuMirror, "Ubuntu"},
	distro.TYPE_LINUX_DISTROS_UBUNTU_PORTS: {state.GetUbuntuPortsMirror, "Ubuntu Ports"},
	distro.TYPE_LINUX_DISTROS_DEBIAN:       {state.GetDebianMirror, "Debian"},
	distro.TYPE_LINUX_DISTROS_CENTOS:       {state.GetCentOSMirror, "CentOS"},
	distro.TYPE_LINUX_DISTROS_ALPINE:       {state.GetAlpineMirror, "Alpine"},
}

// allDistroModes returns all distro type IDs from registry (includes YAML-loaded distros)
func allDistroModes() []int {
	reg := distro.GetRegistry()
	if reg == nil {
		return builtinDistroModes
	}
	all := reg.GetAll()
	modes := make([]int, 0, len(all))
	for _, d := range all {
		if d.Type > 0 {
			modes = append(modes, d.Type)
		}
	}
	if len(modes) == 0 {
		return builtinDistroModes
	}
	return modes
}

func modesToInit(mode int) []int {
	if mode == distro.TYPE_LINUX_ALL_DISTROS {
		return allDistroModes()
	}
	return []int{mode}
}

func getRewriterConfig(mode int) (getMirror func() *url.URL, name string) {
	// Check built-in config first
	if e, ok := builtinRewriterConfig[mode]; ok {
		return e.getMirror, e.name
	}
	// For dynamic distros, use the generic state getter
	distName := distro.DistributionName(mode)
	if distName == "" {
		return nil, ""
	}
	return func() *url.URL {
		return state.Global().GetMirror(mode)
	}, distName
}

// getRewriter returns the rewriter for a given mode from the URLRewriters map
func getRewriter(r *URLRewriters, mode int) *URLRewriter {
	if r.byType == nil {
		return nil
	}
	return r.byType[mode]
}

// setRewriter sets the rewriter for a given mode in the URLRewriters map
func setRewriter(r *URLRewriters, mode int, rw *URLRewriter) {
	if r.byType == nil {
		r.byType = make(map[int]*URLRewriter)
	}
	r.byType[mode] = rw
}

// createRewriter creates a new URLRewriter for a specific distribution.
// It uses the cached benchmark result if available, otherwise runs a synchronous benchmark.
func createRewriter(mode int) *URLRewriter {
	log := logger.Default()
	getMirror, name := getRewriterConfig(mode)
	if getMirror == nil {
		return nil
	}

	benchmarkURL, pattern := mirrors.GetPredefinedConfiguration(mode)
	rewriter := &URLRewriter{pattern: pattern}
	mirror := getMirror()

	if mirror != nil {
		log.Info().Str("distro", name).Str("mirror", mirror.String()).Msg("using specified mirror")
		rewriter.mirror = mirror
		return rewriter
	}

	mirrorURLs := mirrors.GetGeoMirrorUrlsByMode(mode)
	// Use cache-aware benchmark to avoid repeated testing
	fastest, err := benchmarks.GetTheFastestMirrorWithCache(mode, mirrorURLs, benchmarkURL)
	if err != nil {
		log.Error().Err(err).Str("distro", name).Msg("error finding fastest mirror")
		return rewriter
	}

	if mirror, err := url.Parse(fastest); err == nil {
		log.Info().Str("distro", name).Str("mirror", fastest).Msg("using fastest mirror")
		rewriter.mirror = mirror
	}

	return rewriter
}

// createRewriterAsync creates a new URLRewriter for a specific distribution using async benchmark.
// It immediately returns with a default mirror and updates the mirror in the background.
func createRewriterAsync(mode int, rewriters *URLRewriters) *URLRewriter {
	log := logger.Default()
	getMirror, name := getRewriterConfig(mode)
	if getMirror == nil {
		return nil
	}

	benchmarkURL, pattern := mirrors.GetPredefinedConfiguration(mode)
	rewriter := &URLRewriter{pattern: pattern}
	mirror := getMirror()

	if mirror != nil {
		log.Info().Str("distro", name).Str("mirror", mirror.String()).Msg("using specified mirror")
		rewriter.mirror = mirror
		return rewriter
	}

	mirrorURLs := mirrors.GetGeoMirrorUrlsByMode(mode)

	// Check if we have a cached result
	if cached, ok := benchmarks.GetBenchmarkCache().GetCachedResult(mode); ok {
		if parsedMirror, err := url.Parse(cached); err == nil {
			log.Info().Str("distro", name).Str("mirror", cached).Msg("using cached mirror")
			rewriter.mirror = parsedMirror
			return rewriter
		}
	}

	// Use default mirror immediately for fast startup
	defaultMirror := benchmarks.GetDefaultMirror(mirrorURLs)
	if parsedMirror, err := url.Parse(defaultMirror); err == nil {
		log.Info().Str("distro", name).Str("mirror", defaultMirror).Msg("using default mirror (async benchmark pending)")
		rewriter.mirror = parsedMirror
	}

	// Run benchmark in background and update when complete
	benchmarks.GetTheFastestMirrorAsync(mode, mirrorURLs, benchmarkURL, func(result benchmarks.AsyncBenchmarkResult) {
		if result.Error != nil {
			log.Error().Err(result.Error).Str("distro", name).Msg("async benchmark failed")
			return
		}

		parsedMirror, err := url.Parse(result.FastestMirror)
		if err != nil {
			log.Error().Err(err).Str("distro", name).Msg("failed to parse fastest mirror URL")
			return
		}

		// Update the rewriter with the new fastest mirror
		rewriters.Mu.Lock()
		if rw := getRewriter(rewriters, mode); rw != nil {
			rw.mirror = parsedMirror
		}
		rewriters.Mu.Unlock()
		log.Info().Str("distro", name).Str("mirror", result.FastestMirror).Msg("async benchmark completed, mirror updated")
	})

	return rewriter
}

// CreateNewRewriters initializes rewriters based on mode.
// This uses synchronous benchmark which may block startup for up to 30 seconds.
// For faster startup, use CreateNewRewritersAsync instead.
func CreateNewRewriters(mode int) *URLRewriters {
	rewriters := &URLRewriters{byType: make(map[int]*URLRewriter)}
	for _, m := range modesToInit(mode) {
		rw := createRewriter(m)
		if rw != nil {
			setRewriter(rewriters, m, rw)
		}
	}
	return rewriters
}

// CreateNewRewritersAsync initializes rewriters based on mode using async benchmark.
// This allows the server to start immediately with default mirrors while benchmark
// runs in the background. Once benchmark completes, mirrors are automatically updated.
// This is the recommended method for production use to minimize startup time.
func CreateNewRewritersAsync(mode int) *URLRewriters {
	rewriters := &URLRewriters{byType: make(map[int]*URLRewriter)}
	for _, m := range modesToInit(mode) {
		rw := createRewriterAsync(m, rewriters)
		if rw != nil {
			setRewriter(rewriters, m, rw)
		}
	}
	return rewriters
}

// GetRewriteRulesByMode returns caching rules for a specific mode.
// Prefers registry (config-loaded) rules when present.
func GetRewriteRulesByMode(mode int) []distro.Rule {
	if reg := distro.GetRegistry(); reg != nil {
		if d, ok := reg.GetByType(mode); ok && len(d.CacheRules) > 0 {
			return d.CacheRules
		}
	}
	if rules, ok := builtinModeRules[mode]; ok {
		return rules
	}
	n := 0
	for _, r := range builtinModeRules {
		n += len(r)
	}
	rules := make([]distro.Rule, 0, n)
	for _, m := range builtinDistroModes {
		rules = append(rules, builtinModeRules[m]...)
	}
	return rules
}

// RewriteRequestByMode rewrites the request URL to point to the configured mirror
// for the specified distribution mode. It matches the request path against
// distribution-specific patterns and replaces the URL scheme, host, and path
// with the mirror's configuration. If rewriters is nil, the function returns early.
func RewriteRequestByMode(r *http.Request, rewriters *URLRewriters, mode int) {
	if rewriters == nil {
		return
	}
	rewriters.Mu.RLock()
	defer rewriters.Mu.RUnlock()

	rewriter := getRewriter(rewriters, mode)
	if rewriter == nil || rewriter.mirror == nil || rewriter.pattern == nil {
		return
	}

	uri := r.URL.String()
	if !rewriter.pattern.MatchString(uri) {
		return
	}

	r.Header.Add("Content-Location", uri)
	matches := rewriter.pattern.FindStringSubmatch(uri)
	if len(matches) == 0 {
		return
	}

	queryRaw := matches[len(matches)-1]
	unescapedQuery, err := url.PathUnescape(queryRaw)
	if err != nil {
		unescapedQuery = queryRaw
	}

	r.URL.Scheme = rewriter.mirror.Scheme
	r.URL.Host = rewriter.mirror.Host
	if mode == distro.TYPE_LINUX_DISTROS_DEBIAN {
		slugs_query := strings.Split(r.URL.Path, "/")
		slugs_mirror := strings.Split(rewriter.mirror.Path, "/")
		slugs_mirror[0] = slugs_query[0]
		r.URL.Path = strings.Join(slugs_query, "/")
		return
	}
	// Use templates for path construction
	path, err := mirrors.BuildPathWithQuery(rewriter.mirror.Path, unescapedQuery)
	if err != nil {
		// Fallback to concatenation if template fails
		r.URL.Path = rewriter.mirror.Path + unescapedQuery
	} else {
		r.URL.Path = path
	}
}

// MatchingRule finds a matching rule for the given path
func MatchingRule(path string, rules []distro.Rule) (*distro.Rule, bool) {
	for _, rule := range rules {
		if rule.Pattern.MatchString(path) {
			return &rule, true
		}
	}
	return nil, false
}

// RefreshRewriters refreshes the rewriters with updated mirror configurations.
// This function is safe to call concurrently and will update the mirrors
// based on the current state configuration.
// It clears the benchmark cache to force fresh benchmark tests.
//
// IMPORTANT: This function creates new rewriters outside the lock to avoid
// blocking request processing during potentially slow network operations
// (benchmark tests). The lock is only held briefly during the pointer swap.
func RefreshRewriters(rewriters *URLRewriters, mode int) {
	if rewriters == nil {
		return
	}

	log := logger.Default()
	log.Info().Msg("refreshing mirror configurations...")

	// Clear benchmark cache to force fresh tests
	benchmarks.ClearBenchmarkCache()

	// Create new rewriters OUTSIDE the lock to avoid blocking requests
	// during potentially slow network operations (benchmark tests)
	newByMode := make(map[int]*URLRewriter)
	for _, m := range modesToInit(mode) {
		newByMode[m] = createRewriter(m)
	}

	// Only hold the lock briefly during the pointer swap
	rewriters.Mu.Lock()
	for m, rw := range newByMode {
		setRewriter(rewriters, m, rw)
	}
	rewriters.Mu.Unlock()

	log.Info().Msg("mirror configurations refreshed successfully")
}
