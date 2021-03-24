package util

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/terror"
	"io/ioutil"
	"matrixbase/pkg/config"
)

const (
	// syntaxErrorPrefix is the common prefix for SQL syntax error in TiDB.
	syntaxErrorPrefix = "You have an error in your SQL syntax; check the manual that corresponds to your DB version for the right syntax to use"
)

// SyntaxError converts parser error to syntax error.
func SyntaxError(err error) error {
	if err == nil {
		return nil
	}
	// logutil.BgLogger().Debug("syntax error", zap.Error(err))

	// If the error is already a terror with stack, pass it through.
	if errors.HasStack(err) {
		cause := errors.Cause(err)
		if _, ok := cause.(*terror.Error); ok {
			return err
		}
	}

	return parser.ErrParse.GenWithStackByArgs(syntaxErrorPrefix, err.Error())
}

// SyntaxWarn converts parser warn to DB's syntax warn.
func SyntaxWarn(err error) error {
	if err == nil {
		return nil
	}
	return parser.ErrParse.GenWithStackByArgs(syntaxErrorPrefix, err.Error())
}

func LoadTLSCertificates(ca, key, cert string) (tlsConfig *tls.Config, err error) {
	if len(cert) == 0 || len(key) == 0 {
		return
	}

	var tlsCert tls.Certificate
	tlsCert, err = tls.LoadX509KeyPair(cert, key)
	if err != nil {
		// logutil.BgLogger().Warn("load x509 failed", zap.Error(err))
		err = errors.Trace(err)
		return
	}

	requireTLS := config.GetGlobalConfig().Security.RequireSecureTransport

	// Try loading CA cert.
	clientAuthPolicy := tls.NoClientCert
	if requireTLS {
		clientAuthPolicy = tls.RequestClientCert
	}
	var certPool *x509.CertPool
	if len(ca) > 0 {
		var caCert []byte
		caCert, err = ioutil.ReadFile(ca)
		if err != nil {
			// logutil.BgLogger().Warn("read file failed", zap.Error(err))
			err = errors.Trace(err)
			return
		}
		certPool = x509.NewCertPool()
		if certPool.AppendCertsFromPEM(caCert) {
			if requireTLS {
				clientAuthPolicy = tls.RequireAndVerifyClientCert
			} else {
				clientAuthPolicy = tls.VerifyClientCertIfGiven
			}
		}
	}
	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientCAs:    certPool,
		ClientAuth:   clientAuthPolicy,
	}
	return
}

// IsTLSExpiredError checks error is caused by TLS expired.
func IsTLSExpiredError(err error) bool {
	err = errors.Cause(err)
	if inval, ok := err.(x509.CertificateInvalidError); !ok || inval.Reason != x509.Expired {
		return false
	}
	return true
}
