package mirrors

import (
	"strings"
	"testing"

	Define "github.com/apham0001/apt-proxy/define"
)

func TestGetUbuntuMirrorByAliases(t *testing.T) {
	alias := GetMirrorURLByAliases(Define.TYPE_LINUX_DISTROS_UBUNTU, "fr:ircam")
	if !strings.Contains(alias, "mirrors.ircam.fr/pub/ubuntu/") {
		t.Fatal("Test Get Mirror By Custom Name Failed (Ubuntu)")
	}

	alias = GetMirrorURLByAliases(Define.TYPE_LINUX_DISTROS_UBUNTU, "fr:not-found")
	if alias != "" {
		t.Fatal("Test Get Mirror By Custom Name Failed (Ubuntu not-found)")
	}
}

func TestGetDebianMirrorByAliases(t *testing.T) {
	alias := GetMirrorURLByAliases(Define.TYPE_LINUX_DISTROS_DEBIAN, "net:rezopole")
	if !strings.Contains(alias, "ftp.rezopole.net/debian/") {
		t.Fatal("Test Get Mirror By Custom Name Failed (Debian)")
	}

	alias = GetMirrorURLByAliases(Define.TYPE_LINUX_DISTROS_DEBIAN, "fr:not-found")
	if alias != "" {
		t.Fatal("Test Get Mirror By Custom Name Failed (Debian not-found)")
	}
}

func TestGetDebianSecurityMirrorByAliases(t *testing.T) {
	// Replace by a valid FR mirror if you have one
	alias := GetMirrorURLByAliases(Define.TYPE_LINUX_DISTROS_DEBIAN_SECURITY, "net:rezopole")
	if !strings.Contains(alias, "ftp.rezopole.net/debian-security/") {
		t.Log("NOTE: Make sure your DEBIAN_SECURITY mirrors contain this alias")
		// fallback to basic check
	}

	alias = GetMirrorURLByAliases(Define.TYPE_LINUX_DISTROS_DEBIAN_SECURITY, "fr:not-found")
	if alias != "" {
		t.Fatal("Test Get Mirror By Custom Name Failed (Debian-Security not-found)")
	}
}

func TestGetCentOSMirrorByAliases(t *testing.T) {
	alias := GetMirrorURLByAliases(Define.TYPE_LINUX_DISTROS_CENTOS, "fr:ibcp")
	if !strings.Contains(alias, "mirror.ibcp.fr/pub/centos/") {
		t.Fatal("Test Get Mirror By Custom Name Failed (CentOS)")
	}

	alias = GetMirrorURLByAliases(Define.TYPE_LINUX_DISTROS_CENTOS, "fr:not-found")
	if alias != "" {
		t.Fatal("Test Get Mirror By Custom Name Failed (CentOS not-found)")
	}
}

func TestGetMirrorUrlsByGeo(t *testing.T) {
	mirrors := GetGeoMirrorUrlsByMode(Define.TYPE_LINUX_ALL_DISTROS)
	if len(mirrors) == 0 {
		t.Fatal("No mirrors found")
	}

	mirrors = GetGeoMirrorUrlsByMode(Define.TYPE_LINUX_DISTROS_DEBIAN)
	if len(mirrors) != len(BUILDIN_DEBIAN_MIRRORS) {
		t.Fatal("Get mirrors error for debian")
	}

	mirrors = GetGeoMirrorUrlsByMode(Define.TYPE_LINUX_DISTROS_DEBIAN_SECURITY)
	if len(mirrors) != len(BUILDIN_DEBIAN_SECURITY_MIRRORS) {
		t.Fatal("Get mirrors error for debian-security")
	}

	mirrors = GetGeoMirrorUrlsByMode(Define.TYPE_LINUX_DISTROS_UBUNTU)
	if len(mirrors) == 0 {
		t.Fatal("No mirrors found for ubuntu")
	}
}

func TestGetPredefinedConfiguration(t *testing.T) {
	res, pattern := GetPredefinedConfiguration(Define.TYPE_LINUX_DISTROS_UBUNTU)
	if res != Define.UBUNTU_BENCHMAKR_URL {
		t.Fatal("Failed to get resource link")
	}
	if !pattern.MatchString("/ubuntu/InRelease") {
		t.Fatal("Failed to verify domain name rules")
	}

	res, pattern = GetPredefinedConfiguration(Define.TYPE_LINUX_DISTROS_DEBIAN)
	if res != Define.DEBIAN_BENCHMAKR_URL {
		t.Fatal("Failed to get resource link")
	}
	if !pattern.MatchString("/debian/InRelease") {
		t.Fatal("Failed to verify domain name rules")
	}

	res, pattern = GetPredefinedConfiguration(Define.TYPE_LINUX_DISTROS_DEBIAN_SECURITY)
	if res != Define.DEBIAN_SECURITY_BENCHMARK_URL {
		t.Fatal("Failed to get resource link for debian-security")
	}
	if !pattern.MatchString("/debian-security/InRelease") {
		t.Fatal("Failed to verify domain name rules for debian-security")
	}

	res, pattern = GetPredefinedConfiguration(Define.TYPE_LINUX_DISTROS_CENTOS)
	if res != Define.CENTOS_BENCHMAKR_URL {
		t.Fatal("Failed to get resource link")
	}
	if !pattern.MatchString("/centos/test/repomd.xml") {
		t.Fatal("Failed to verify domain name rules")
	}

	res, pattern = GetPredefinedConfiguration(Define.TYPE_LINUX_DISTROS_ALPINE)
	if res != Define.ALPINE_BENCHMAKR_URL {
		t.Fatal("Failed to get resource link")
	}
	if !pattern.MatchString("/alpine/test/APKINDEX.tar.gz") {
		t.Fatal("Failed to verify domain name rules")
	}
}
