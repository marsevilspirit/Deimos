package transport

import (
	"crypto/tls"
	"errors"
	"io/ioutil"
	"os"
	"testing"
)

func createTempFile(b []byte) (string, error) {
	f, err := ioutil.TempFile("", "deimos-test-tls-")
	if err != nil {
		return "", err
	}
	defer f.Close()

	if _, err = f.Write(b); err != nil {
		return "", err
	}

	return f.Name(), nil
}

func fakeCertificateParserFunc(cert tls.Certificate, err error) func(certPEMBlock, keyPEMBlock []byte) (tls.Certificate, error) {
	return func(certPEMBlock, keyPEMBlock []byte) (tls.Certificate, error) {
		return cert, err
	}
}

func TestNewTransportTLSInfo(t *testing.T) {
	tmp, err := createTempFile([]byte("XXX"))
	if err != nil {
		t.Fatalf("Unable to prepare tmpfile: %v", err)
	}
	defer os.Remove(tmp)

	tests := []struct {
		info                TLSInfo
		wantTLSClientConfig bool
	}{
		{
			info:                TLSInfo{},
			wantTLSClientConfig: false,
		},
		{
			info: TLSInfo{
				CertFile: tmp,
				KeyFile:  tmp,
			},
			wantTLSClientConfig: true,
		},
		{
			info: TLSInfo{
				CertFile: tmp,
				KeyFile:  tmp,
				CAFile:   tmp,
			},
			wantTLSClientConfig: true,
		},
	}

	for i, tt := range tests {
		tt.info.parseFunc = fakeCertificateParserFunc(tls.Certificate{}, nil)
		trans, err := NewTransport(tt.info)
		if err != nil {
			t.Fatalf("Received unexpected error from NewTransport: %v", err)
		}

		gotTLSClientConfig := trans.TLSClientConfig != nil
		if tt.wantTLSClientConfig != gotTLSClientConfig {
			t.Fatalf("#%d: wantTLSClientConfig=%t but gotTLSClientConfig=%t", i, tt.wantTLSClientConfig, gotTLSClientConfig)
		}
	}
}

func TestTLSInfoEmpty(t *testing.T) {
	tests := []struct {
		info TLSInfo
		want bool
	}{
		{TLSInfo{}, true},
		{TLSInfo{CAFile: "baz"}, true},
		{TLSInfo{CertFile: "foo"}, false},
		{TLSInfo{KeyFile: "bar"}, false},
		{TLSInfo{CertFile: "foo", KeyFile: "bar"}, false},
		{TLSInfo{CertFile: "foo", CAFile: "baz"}, false},
		{TLSInfo{KeyFile: "bar", CAFile: "baz"}, false},
		{TLSInfo{CertFile: "foo", KeyFile: "bar", CAFile: "baz"}, false},
	}

	for i, tt := range tests {
		got := tt.info.Empty()
		if tt.want != got {
			t.Errorf("#%d: result of Empty() incorrect: want=%t got=%t", i, tt.want, got)
		}
	}
}

func TestTLSInfoMissingFields(t *testing.T) {
	tmp, err := createTempFile([]byte("XXX"))
	if err != nil {
		t.Fatalf("Unable to prepare tmpfile: %v", err)
	}
	defer os.Remove(tmp)

	tests := []TLSInfo{
		TLSInfo{},
		TLSInfo{CAFile: tmp},
		TLSInfo{CertFile: tmp},
		TLSInfo{KeyFile: tmp},
		TLSInfo{CertFile: tmp, CAFile: tmp},
		TLSInfo{KeyFile: tmp, CAFile: tmp},
	}

	for i, info := range tests {
		if _, err := info.ServerConfig(); err == nil {
			t.Errorf("#%d: expected non-nil error from ServerConfig()", i)
		}

		if _, err = info.ClientConfig(); err == nil {
			t.Errorf("#%d: expected non-nil error from ClientConfig()", i)
		}
	}
}

func TestTLSInfoParseFuncError(t *testing.T) {
	tmp, err := createTempFile([]byte("XXX"))
	if err != nil {
		t.Fatalf("Unable to prepare tmpfile: %v", err)
	}
	defer os.Remove(tmp)

	info := TLSInfo{CertFile: tmp, KeyFile: tmp, CAFile: tmp}
	info.parseFunc = fakeCertificateParserFunc(tls.Certificate{}, errors.New("fake"))

	if _, err := info.ServerConfig(); err == nil {
		t.Errorf("expected non-nil error from ServerConfig()")
	}

	if _, err = info.ClientConfig(); err == nil {
		t.Errorf("expected non-nil error from ClientConfig()")
	}
}

func TestTLSInfoConfigFuncs(t *testing.T) {
	tmp, err := createTempFile([]byte("XXX"))
	if err != nil {
		t.Fatalf("Unable to prepare tmpfile: %v", err)
	}
	defer os.Remove(tmp)

	tests := []struct {
		info       TLSInfo
		clientAuth tls.ClientAuthType
		wantCAs    bool
	}{
		{
			info:       TLSInfo{CertFile: tmp, KeyFile: tmp},
			clientAuth: tls.NoClientCert,
			wantCAs:    false,
		},

		{
			info:       TLSInfo{CertFile: tmp, KeyFile: tmp, CAFile: tmp},
			clientAuth: tls.RequireAndVerifyClientCert,
			wantCAs:    true,
		},
	}

	for i, tt := range tests {
		tt.info.parseFunc = fakeCertificateParserFunc(tls.Certificate{}, nil)

		sCfg, err := tt.info.ServerConfig()
		if err != nil {
			t.Errorf("#%d: expected nil error from ServerConfig(), got non-nil: %v", i, err)
		}

		if tt.wantCAs != (sCfg.ClientCAs != nil) {
			t.Errorf("#%d: wantCAs=%t but ClientCAs=%v", i, tt.wantCAs, sCfg.ClientCAs)
		}

		cCfg, err := tt.info.ClientConfig()
		if err != nil {
			t.Errorf("#%d: expected nil error from ClientConfig(), got non-nil: %v", i, err)
		}

		if tt.wantCAs != (cCfg.RootCAs != nil) {
			t.Errorf("#%d: wantCAs=%t but RootCAs=%v", i, tt.wantCAs, sCfg.RootCAs)
		}
	}
}
