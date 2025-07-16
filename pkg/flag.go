package pkg

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/marsevilspirit/deimos/pkg/flags"
	"github.com/marsevilspirit/deimos/pkg/transport"
)

// SetFlagsFromEnv parses all registered flags in the given flagset,
// and if they are not already set it attempts to set their values from
// environment variables. Environment variables take the name of the flag but
// are UPPERCASE, have the prefix "DEIMOS_", and any dashes are replaced by
// underscores - for example: some-flag => DEIMOS_SOME_FLAG
func SetFlagsFromEnv(fs *flag.FlagSet) {
	alreadySet := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		alreadySet[f.Name] = true
	})
	fs.VisitAll(func(f *flag.Flag) {
		if !alreadySet[f.Name] {
			key := "DEIMOS_" + strings.ToUpper(strings.Replace(f.Name, "-", "_", -1))
			val := os.Getenv(key)
			if val != "" {
				fs.Set(f.Name, val)
			}
		}
	})
}

func URLsFromFlags(fs *flag.FlagSet, urlsFlagName string, addrFlagName string, tlsInfo transport.TLSInfo) ([]url.URL, error) {
	visited := make(map[string]struct{})
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = struct{}{}
	})

	_, urlsFlagIsSet := visited[urlsFlagName]
	_, addrFlagIsSet := visited[addrFlagName]

	if addrFlagIsSet {
		if urlsFlagIsSet {
			return nil, fmt.Errorf("Set only one of flags -%s and -%s", urlsFlagName, addrFlagName)
		}

		addr := *fs.Lookup(addrFlagName).Value.(*flags.IPAddressPort)
		addrURL := url.URL{Scheme: "http", Host: addr.String()}
		if !tlsInfo.Empty() {
			addrURL.Scheme = "https"
		}
		return []url.URL{addrURL}, nil
	}

	return []url.URL(*fs.Lookup(urlsFlagName).Value.(*flags.URLsValue)), nil
}
