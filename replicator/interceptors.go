package replicator

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	reqIDKey = "request_id"
)

// maskClientPort removes the port from IP addresses.
func maskClientPort(address string) string {
	if idx := strings.LastIndex(address, ":"); idx != -1 {
		return address[:idx] // Remove port
	}
	return address
}

// getClientIP extracts and masks the client IP (removes port).
func getClientIP(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		ip := p.Addr.String()
		return maskClientPort(ip)
	}
	return "unknown"
}

func getNamespace(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := md.Get("x-namespace"); len(values) > 0 {
		return values[0]
	}
	return ""
}

// extractRequestID retrieves x-request-id or x-trace-id from gRPC metadata.
// If not present, it generates a new one.
func extractRequestID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return generateNewRequestID()
	}

	// Try to get `x-request-id`
	if values := md.Get("x-request-id"); len(values) > 0 {
		return values[0]
	}

	// Try to get `x-trace-id`
	if values := md.Get("x-trace-id"); len(values) > 0 {
		return values[0]
	}

	// If missing, generate a new request ID
	return generateNewRequestID()
}

// generateNewRequestID creates a new UUID-based request ID.
func generateNewRequestID() string {
	return uuid.New().String()
}

// wrappedServerStream wraps gRPC ServerStream to modify the context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// RequestIDStreamInterceptor ensures every streaming gRPC request has a request ID.
func RequestIDStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// Extract or generate request ID
	requestID := extractRequestID(ss.Context())

	// Attach request ID to outgoing context
	newCtx := metadata.AppendToOutgoingContext(ss.Context(), "x-request-id", requestID)
	wrappedStream := &wrappedServerStream{ServerStream: ss, ctx: newCtx}

	// Attach request ID to response metadata
	if err := ss.SetHeader(metadata.Pairs("x-request-id", requestID)); err != nil {
		return err
	}

	// Process streaming request
	return handler(srv, wrappedStream)
}

// CorrelationIDStreamInterceptor extracts or generates `X-Correlation-ID` and propagates it.
func CorrelationIDStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	md, ok := metadata.FromIncomingContext(ss.Context())
	var correlationID string

	if ok {
		// Check for existing x-correlation-id
		if values := md.Get("x-correlation-id"); len(values) > 0 {
			correlationID = values[0]
		}

		// If missing, check for x-request-id
		if correlationID == "" {
			if values := md.Get("x-request-id"); len(values) > 0 {
				correlationID = values[0]
			}
		}
	}

	// Attach Correlation ID to outgoing context
	newCtx := metadata.AppendToOutgoingContext(ss.Context(), "x-correlation-id", correlationID)
	wrappedStream := &wrappedServerStream{ServerStream: ss, ctx: newCtx}

	// Attach Correlation ID to response metadata
	if err := wrappedStream.SetHeader(metadata.Pairs("x-correlation-id", correlationID)); err != nil {
		return err
	}

	// Process streaming request
	return handler(srv, wrappedStream)
}

// TelemetryInterceptor logs gRPC requests, responses, and status codes and basic metrics.
func TelemetryInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	startTime := time.Now()
	clientIP := getClientIP(ss.Context())
	requestID := extractRequestID(ss.Context())
	ns := getNamespace(ss.Context())
	slog.Info("[GRPC] Streaming Request Started",
		"method", info.FullMethod,
		"client_ip", clientIP,
		reqIDKey, requestID,
		"namespace", ns,
	)
	metricsActiveStreams.WithLabelValues(ns, info.FullMethod).Inc()
	// handler
	err := handler(srv, ss)

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	st, _ := status.FromError(err)
	grpcStatus := st.Code().String()
	// Record duration in Prometheus
	rpcDurations.WithLabelValues(ns, info.FullMethod, grpcStatus).Observe(duration.Seconds())
	metricsActiveStreams.WithLabelValues(ns, info.FullMethod).Dec()
	// Log completion.
	if err != nil {
		slog.Error("[GRPC] Streaming Request Failed",
			"method", info.FullMethod,
			"client_ip", clientIP,
			"status", grpcStatus,
			"duration_sec", duration.Seconds(),
			"error", err.Error(),
			reqIDKey, requestID,
			"namespace", ns,
		)
	} else {
		slog.Info("[GRPC] Streaming Request Completed",
			"method", info.FullMethod,
			"client_ip", clientIP,
			"status", grpcStatus,
			"duration_sec", duration.Seconds(),
			reqIDKey, requestID,
			"namespace", ns,
		)
	}

	return err
}

func RequireNamespaceInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	md, ok := metadata.FromIncomingContext(ss.Context())

	// Check if metadata exists
	if !ok || len(md.Get("x-namespace")) == 0 {
		return toGRPCError(ErrMissingNamespaceInMetadata)
	}

	namespace := md.Get("x-namespace")[0]

	ctx := metadata.AppendToOutgoingContext(ss.Context(), "x-namespace", namespace)
	wrappedStream := &wrappedServerStream{ServerStream: ss, ctx: ctx}

	return handler(srv, wrappedStream)
}

func MethodInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := metadata.AppendToOutgoingContext(ss.Context(), "x-method", info.FullMethod)
	wrappedStream := &wrappedServerStream{ServerStream: ss, ctx: ctx}

	return handler(srv, wrappedStream)
}

func getMethod(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := md.Get("x-method"); len(values) > 0 {
		return values[0]
	}
	return ""
}
