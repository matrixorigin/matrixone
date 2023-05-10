package proxy

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plugin"
	"time"
)

// pluginRouter is a router implementation that uses external plugin to select CN server.
type pluginRouter struct {
	// Router is a delegated impl that is used when plugin flags a Bypass action
	Router
	// plugin is the plugin that is used to select CN server
	plugin Plugin
}

func newPluginRouter(r Router, backend string, timeout time.Duration) (*pluginRouter, error) {
	p, err := newRPCPlugin(backend, timeout)
	if err != nil {
		return nil, err
	}
	return &pluginRouter{
		Router: r,
		plugin: p,
	}, nil
}

// Route implements Router.Route.
func (r *pluginRouter) Route(ctx context.Context, ci clientInfo) (*CNServer, error) {
	re, err := r.plugin.RecommendCN(ctx, ci)
	if err != nil {
		return nil, err
	}
	switch re.Action {
	case plugin.Select:
		if re.CN == nil {
			return nil, moerr.NewInternalErrorNoCtx("no CN server selected")
		}
		hash, err := ci.labelInfo.getHash()
		if err != nil {
			return nil, err
		}
		return &CNServer{
			reqLabel: ci.labelInfo,
			cnLabel:  re.CN.Labels,
			uuid:     re.CN.ServiceID,
			addr:     re.CN.SQLAddress,
			hash:     hash,
		}, nil
	case plugin.Reject:
		return nil, withCode(moerr.NewInfoNoCtx(re.Message),
			codeAuthFailed)
	case plugin.Bypass:
		return r.Router.Route(ctx, ci)
	default:
		return nil, moerr.NewInternalErrorNoCtx("unknown recommended action %d", re.Action)
	}
}

// Plugin is the interface of proxy plugin.
type Plugin interface {
	// RecommendCN returns the recommended CN server.
	RecommendCN(ctx context.Context, client clientInfo) (*plugin.Recommendation, error)
}

type rpcPlugin struct {
	client  morpc.RPCClient
	backend string
	timeout time.Duration
}

func newRPCPlugin(backend string, timeout time.Duration) (*rpcPlugin, error) {
	codec := morpc.NewMessageCodec(func() morpc.Message {
		return &plugin.Response{}
	})
	backendOpts := []morpc.BackendOption{
		morpc.WithBackendConnectTimeout(timeout),
		morpc.WithBackendHasPayloadResponse(),
		morpc.WithBackendLogger(logutil.GetGlobalLogger().Named("plugin-backend")),
	}
	bf := morpc.NewGoettyBasedBackendFactory(codec, backendOpts...)

	clientOpts := []morpc.ClientOption{
		morpc.WithClientInitBackends([]string{backend}, []int{1}),
		morpc.WithClientMaxBackendPerHost(1),
		morpc.WithClientTag("plugin-client"),
		morpc.WithClientLogger(logutil.GetGlobalLogger()),
	}
	cli, err := morpc.NewClient(bf, clientOpts...)
	if err != nil {
		return nil, err
	}
	return &rpcPlugin{client: cli, backend: backend, timeout: timeout}, nil
}

func (p *rpcPlugin) RecommendCN(ctx context.Context, ci clientInfo) (*plugin.Recommendation, error) {

	resp, err := p.request(ctx, &plugin.Request{ClientInfo: &plugin.ClientInfo{
		Tenant:        string(ci.Tenant),
		Username:      ci.username,
		OriginIP:      ci.originIP.String(),
		LabelSelector: ci.labelInfo.allLabels(),
	}})
	if err != nil {
		return nil, err
	}
	return resp.Recommendation, nil
}

func (p *rpcPlugin) request(ctx context.Context, req *plugin.Request) (*plugin.Response, error) {
	cc, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	f, err := p.client.Send(cc, p.backend, req)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	resp, err := f.Get()
	if err != nil {
		return nil, err
	}
	return resp.(*plugin.Response), nil
}
