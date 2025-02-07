module github.com/ankur-anand/kvalchemy

go 1.23.2

require (
	github.com/bits-and-blooms/bloom/v3 v3.7.0
	github.com/brianvoe/gofakeit/v7 v7.2.1
	github.com/dgraph-io/badger/v4 v4.5.1
	github.com/gofrs/flock v0.12.1
	github.com/google/flatbuffers v24.12.23+incompatible
	github.com/google/uuid v1.6.0
	github.com/pelletier/go-toml/v2 v2.2.3
	github.com/pierrec/lz4/v4 v4.1.22
	github.com/prometheus/client_golang v1.20.5
	github.com/rosedblabs/wal v1.3.8
	github.com/stretchr/testify v1.10.0
	go.etcd.io/bbolt v1.4.0
	golang.org/x/sync v0.10.0
	google.golang.org/grpc v1.70.0
	google.golang.org/protobuf v1.36.4
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.10.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto/v2 v2.1.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// replacing for #https://github.com/rosedblabs/wal/pull/45
replace github.com/rosedblabs/wal v1.3.8 => github.com/ankur-anand/wal v0.0.1
