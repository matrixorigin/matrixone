package plan

import "context"

type viewDescriptionCompilerContextKey struct{}

// WithViewDescriptionCompilerContext attaches a request-scoped compiler context
// for origin-CN metadata operators. Context values are not a wire contract.
func WithViewDescriptionCompilerContext(ctx context.Context, compiler CompilerContext) context.Context {
	if compiler == nil {
		return ctx
	}
	return context.WithValue(ctx, viewDescriptionCompilerContextKey{}, compiler)
}

func ViewDescriptionCompilerContext(ctx context.Context) CompilerContext {
	compiler, _ := ctx.Value(viewDescriptionCompilerContextKey{}).(CompilerContext)
	return compiler
}
