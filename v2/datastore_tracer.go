package dht

import (
	"context"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-datastore/trace"
	"go.opentelemetry.io/otel/codes"
	otel "go.opentelemetry.io/otel/trace"
)

type DatastoreWithTracer struct {
	ds              DatastoreWithGetAll
	tracer          otel.Tracer
	tracedDataStore trace.Datastore
}

func NewDatastoreWithTracer(ds DatastoreWithGetAll, tracer otel.Tracer) *DatastoreWithTracer {
	t := *trace.New(ds, tracer)
	return &DatastoreWithTracer{
		ds:              ds,
		tracer:          tracer,
		tracedDataStore: t,
	}
}

func (t *DatastoreWithTracer) GetAll(ctx context.Context) (value map[ds.Key]string, err error) {
	ctx, span := t.tracer.Start(ctx, "GetAll")
	defer span.End()

	value, err = t.ds.GetAll(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return value, err
}

func (t *DatastoreWithTracer) Put(ctx context.Context, key ds.Key, value []byte) error {
	return t.tracedDataStore.Put(ctx, key, value)
}

func (t *DatastoreWithTracer) Sync(ctx context.Context, key ds.Key) error {
	return t.tracedDataStore.Sync(ctx, key)
}

func (t *DatastoreWithTracer) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	return t.tracedDataStore.Get(ctx, key)
}

func (t *DatastoreWithTracer) Has(ctx context.Context, key ds.Key) (bool, error) {
	return t.tracedDataStore.Has(ctx, key)
}

func (t *DatastoreWithTracer) GetSize(ctx context.Context, key ds.Key) (int, error) {
	return t.tracedDataStore.GetSize(ctx, key)
}

func (t *DatastoreWithTracer) Delete(ctx context.Context, key ds.Key) error {
	return t.tracedDataStore.Delete(ctx, key)
}

func (t *DatastoreWithTracer) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	return t.tracedDataStore.Query(ctx, q)
}

func (t *DatastoreWithTracer) Batch(ctx context.Context) (ds.Batch, error) {
	return t.tracedDataStore.Batch(ctx)
}

func (t *DatastoreWithTracer) Close() error {
	return t.tracedDataStore.Close()
}

func (t *DatastoreWithTracer) NewTransaction(ctx context.Context, readOnly bool) (ds.Txn, error) {
	return t.tracedDataStore.NewTransaction(ctx, readOnly)
}
