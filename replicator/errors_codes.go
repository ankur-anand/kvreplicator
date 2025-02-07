package replicator

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrMissingNamespace           = errors.New("missing required parameter: namespace")
	ErrNamespaceNotExists         = errors.New("namespace is not found on the current server")
	ErrInvalidMetadata            = errors.New("bad metadata in request")
	ErrMissingNamespaceInMetadata = errors.New("missing required metadata: x-namespace")
	ErrStreamTimeout              = errors.New("stream timeout: no records received within 60 seconds")
	ErrStatusOk                   = status.Error(codes.OK, "")
)

// Convert business error to gRPC error.
func toGRPCError(err error) error {
	switch {
	case errors.Is(err, ErrMissingNamespace):
		return status.Error(codes.InvalidArgument, ErrMissingNamespace.Error())
	case errors.Is(err, ErrNamespaceNotExists):
		return status.Error(codes.NotFound, ErrNamespaceNotExists.Error())
	case errors.Is(err, ErrInvalidMetadata):
		return status.Error(codes.InvalidArgument, ErrInvalidMetadata.Error())
	case errors.Is(err, ErrMissingNamespaceInMetadata):
		return status.Error(codes.InvalidArgument, ErrMissingNamespaceInMetadata.Error())
	case errors.Is(err, ErrStreamTimeout):
		return status.Error(codes.DeadlineExceeded, ErrStreamTimeout.Error())
	default:
		return status.Errorf(codes.Internal, "internal server error")
	}
}
