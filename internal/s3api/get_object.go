package s3api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

func (h *Handler) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	// HeadObject is a pure metadata lookup (no PBS call), so use it to learn
	// the object's size up front and decide whether/how to slice a Range
	// *before* asking the backend for the body — that lets the backend
	// stream a full-object GET directly rather than always materializing a
	// local file first "just in case" a range was requested.
	info, err := h.Backend.HeadObject(r.Context(), bucket, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErrorCode(w, r, "NoSuchKey", "the specified key does not exist")
			return
		}
		writeInternalError(w, r, err)
		return
	}

	var rangeSpec *RangeSpec
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		start, end, ok := parseRange(rangeHeader, info.Size)
		if !ok {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", info.Size))
			writeErrorCode(w, r, "InvalidArgument", "invalid Range header")
			return
		}
		rangeSpec = &RangeSpec{Start: start, End: end}
	}

	body, _, err := h.Backend.GetObject(r.Context(), bucket, key, rangeSpec)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErrorCode(w, r, "NoSuchKey", "the specified key does not exist")
			return
		}
		writeInternalError(w, r, err)
		return
	}
	defer body.Close()

	w.Header().Set("ETag", quoteETag(info.ETag))
	w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")

	if rangeSpec == nil {
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, body)
		return
	}

	length := rangeSpec.End - rangeSpec.Start + 1
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rangeSpec.Start, rangeSpec.End, info.Size))
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = io.Copy(w, body)
}

// parseRange parses a single-range "bytes=start-end" Range header value.
// Multi-range requests are not supported and cause ok=false, which callers
// treat as an invalid range (S3 clients used by Wings only ever request a
// single contiguous range).
func parseRange(header string, size int64) (start, end int64, ok bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(header, prefix) {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(header, prefix)
	if strings.Contains(spec, ",") {
		return 0, 0, false
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}

	if parts[0] == "" {
		// suffix range: last N bytes
		n, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, true
	}

	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	if parts[1] == "" {
		end = size - 1
	} else {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, false
		}
	}
	if end >= size {
		end = size - 1
	}
	if start > end || size == 0 {
		return 0, 0, false
	}
	return start, end, true
}
