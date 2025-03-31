package test

import (
    "context"
    "errors"
    "github.com/nats-io/nats.go/jetstream"
    "github.com/nats-io/nkeys"
    "github.com/nats-io/nuid"
    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"
    "github.com/synadia-io/orbit.go/natsstore"
)

type data struct {
    Value string
}

func testTreeGenerator(codec natsstore.Codec) func() {
    return func() {
        var bucket string
        var store natsstore.Store

        BeforeEach(func() {
            bucket = nuid.Next()

            var err error
            _, err = js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
                Bucket: bucket,
            })

            store, err = natsstore.NewKeyValueStore(js, bucket, codec)
            Expect(err).ToNot(HaveOccurred())
        })

        AfterEach(func() {
            err := js.DeleteKeyValue(context.Background(), bucket)
            Expect(err).ToNot(HaveOccurred())
        })

        Describe("listing entries", func() {
            Context("when there are no entries", func() {
                It("should return an empty list", func() {
                    err := store.List(context.Background(), ">", func(entry natsstore.Entry, hasMore bool) error {
                        Expect(entry.IsNew()).To(BeTrue())
                        Expect(hasMore).To(BeFalse())
                        return nil
                    })
                    Expect(err).ToNot(HaveOccurred())
                })
            })

            Context("when there are entries", func() {
                BeforeEach(func() {
                    _, err := store.Apply(context.Background(), "meta.store.user-a.entry1", func(entry natsstore.Entry) ([]byte, error) {
                        return []byte("data1"), nil
                    })
                    Expect(err).ToNot(HaveOccurred())

                    _, err = store.Apply(context.Background(), "meta.store.user-b.entry2", func(entry natsstore.Entry) ([]byte, error) {
                        return []byte("data2"), nil
                    })
                    Expect(err).ToNot(HaveOccurred())
                })

                It("should error if no filter is provided", func() {
                    err := store.List(context.Background(), "", func(entry natsstore.Entry, hasMore bool) error {
                        Fail("Should not have called the handler")
                        return nil
                    })
                    Expect(err).To(HaveOccurred())
                })
                It("should return entries matching the filter", func() {
                    resultCount := 0
                    err := store.List(context.Background(), "meta.store.user-b.*", func(entry natsstore.Entry, hasMore bool) error {
                        if !entry.IsNew() {
                            resultCount++
                        }

                        return nil
                    })
                    Expect(err).ToNot(HaveOccurred())
                    Expect(resultCount).To(Equal(1))
                })
            })
        })
        Describe("checking if an entry exists", func() {
            When("the entry does not exist", func() {
                It("should return false", func() {
                    fnd, err := store.Exists(context.Background(), "meta.store.user-a.unknown")
                    Expect(err).ToNot(HaveOccurred())
                    Expect(fnd).To(BeFalse())
                })
            })
            When("the entry exists", func() {
                BeforeEach(func(ctx context.Context) {
                    _, err := store.Apply(ctx, "meta.store.user-a.entry1", func(entry natsstore.Entry) ([]byte, error) {
                        return []byte("data1"), nil
                    })
                    Expect(err).ToNot(HaveOccurred())
                })
                AfterEach(func(ctx context.Context) {
                    var keys []string
                    err := store.List(context.Background(), "meta.store.*.*", func(entry natsstore.Entry, hasMore bool) error {
                        keys = append(keys, entry.Key)
                        return nil
                    })
                    Expect(err).ToNot(HaveOccurred())

                    for _, key := range keys {
                        err = store.Purge(ctx, key)
                        Expect(err).ToNot(HaveOccurred())
                    }
                })

                It("should return true", func() {
                    fnd, err := store.Exists(context.Background(), "meta.store.user-a.entry1")
                    Expect(err).ToNot(HaveOccurred())
                    Expect(fnd).To(BeTrue())
                })
            })
        })
        Describe("applying a mutation", func() {
            When("the mutator results in an error", func() {
                It("should return an error", func() {
                    _, err := store.Apply(context.Background(), "meta.store.user-a.entry1", func(entry natsstore.Entry) ([]byte, error) {
                        return nil, errors.New("error")
                    })
                    Expect(err).To(HaveOccurred())
                })
            })

            When("the entry does not exist", func() {
                It("should create the entry", func() {
                    _, err := store.Apply(context.Background(), "meta.store.user-a.unknown", func(entry natsstore.Entry) ([]byte, error) {
                        if !entry.IsNew() {
                            Fail("Entry should be new")
                        }
                        return nil, nil
                    })
                    Expect(err).ToNot(HaveOccurred())
                })
            })

            When("the entry exists", func() {
                BeforeEach(func() {
                    _, err := store.Apply(context.Background(), "meta.store.user-a.entry1", func(entry natsstore.Entry) ([]byte, error) {
                        return entry.Encode(data{Value: "data1"})
                    })
                    Expect(err).ToNot(HaveOccurred())
                })

                It("should update the entry", func() {
                    _, err := store.Apply(context.Background(), "meta.store.user-a.entry1", func(entry natsstore.Entry) ([]byte, error) {
                        return entry.Encode(data{Value: "data2"})
                    })
                    Expect(err).ToNot(HaveOccurred())

                    var entry *natsstore.Entry
                    entry, _, err = store.Get(context.Background(), "meta.store.user-a.entry1")

                    var d data
                    _ = entry.Decode(&d)

                    Expect(err).ToNot(HaveOccurred())
                    Expect(d.Value).To(Equal("data2"))
                })
            })
        })
        Describe("retrieving an entry", func() {
            When("the entry does not exist", func() {
                It("should return nil, false, and no error", func() {
                    entry, fnd, err := store.Get(context.Background(), "meta.store.user-a.unknown")

                    Expect(err).NotTo(HaveOccurred())
                    Expect(entry).To(BeNil())
                    Expect(fnd).To(BeFalse())
                })
            })
            When("the entry exists", func() {
                BeforeEach(func() {
                    _, err := store.Apply(context.Background(), "meta.store.user-a.entry1", func(entry natsstore.Entry) ([]byte, error) {
                        return entry.Encode(data{Value: "data1"})
                    })
                    Expect(err).ToNot(HaveOccurred())
                })

                It("should return the entry", func() {
                    entry, fnd, err := store.Get(context.Background(), "meta.store.user-a.entry1")
                    var d data
                    _ = entry.Decode(&d)

                    Expect(err).NotTo(HaveOccurred())
                    Expect(entry.Revision).ToNot(Equal(uint64(0)))
                    Expect(entry.Key).To(Equal("meta.store.user-a.entry1"))
                    Expect(fnd).To(BeTrue())
                    Expect(d.Value).To(Equal("data1"))
                })
            })
        })
        Describe("removing an entry", func() {
            When("the entry does not exist", func() {
                It("should return no error", func() {
                    err := store.Delete(context.Background(), "meta.store.user-a.unknown")
                    Expect(err).NotTo(HaveOccurred())
                })
            })
            When("the entry exists", func() {
                BeforeEach(func() {
                    _, err := store.Apply(context.Background(), "meta.store.user-a.entry1", func(entry natsstore.Entry) ([]byte, error) {
                        return entry.Encode(data{Value: "data1"})
                    })
                    Expect(err).ToNot(HaveOccurred())
                })

                It("should remove the entry", func() {
                    err := store.Delete(context.Background(), "meta.store.user-a.entry1")
                    Expect(err).NotTo(HaveOccurred())

                    _, fnd, err := store.Get(context.Background(), "meta.store.user-a.entry1")
                    Expect(err).NotTo(HaveOccurred())
                    Expect(fnd).To(BeFalse())
                })
            })
        })
        Describe("purging an entry", func() {
            When("the entry does not exist", func() {
                It("should return no error", func() {
                    err := store.Purge(context.Background(), "meta.store.user-a.unknown")
                    Expect(err).NotTo(HaveOccurred())
                })
            })
            When("the entry exists", func() {
                BeforeEach(func(ctx context.Context) {
                    _, err := store.Apply(context.Background(), "meta.store.user-a.entry1", func(entry natsstore.Entry) ([]byte, error) {
                        return entry.Encode(data{Value: "data1"})
                    })
                    Expect(err).ToNot(HaveOccurred())
                })
                AfterEach(func(ctx context.Context) {
                    var keys []string
                    err := store.List(context.Background(), "meta.store.*.*", func(entry natsstore.Entry, hasMore bool) error {
                        keys = append(keys, entry.Key)
                        return nil
                    })
                    Expect(err).ToNot(HaveOccurred())

                    for _, key := range keys {
                        err = store.Purge(ctx, key)
                        Expect(err).ToNot(HaveOccurred())
                    }
                })

                It("should remove the entry", func() {
                    err := store.Purge(context.Background(), "meta.store.user-a.entry1")
                    Expect(err).NotTo(HaveOccurred())

                    fnd, err := store.Exists(context.Background(), "meta.store.user-a.entry1")
                    Expect(err).NotTo(HaveOccurred())
                    Expect(fnd).To(BeFalse())
                })
            })
        })
    }
}

var _ = Describe("KeyValue", func() {
    var xkey, _ = nkeys.CreateCurveKeys()

    Context("using the json codec", testTreeGenerator(natsstore.NewJsonCodec()))
    Context("using a secured json codec", testTreeGenerator(natsstore.Secured(natsstore.NewJsonCodec(), xkey)))
})
