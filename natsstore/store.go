package natsstore

import (
    "context"
)

// EntryHandler is a function that is called for each entry in the store that matches the filter.
// A handler must be able to process 'new' entries. These entries have not been stored yet, have a revision of 0 and
// no data attached to it. The hasMore parameter is true if there are more entries to process.
type EntryHandler func(entry Entry, hasMore bool) error

// Mutator is a function that can mutate an entry. 
// It returns the new data for the entry or an error if the mutation failed. If the data is nil, the 
// entry will be deleted from the store.
type Mutator func(entry Entry) ([]byte, error)

// Store represents a key-value store that can be used to store and retrieve entries.
type Store interface {
    // List calls the handler for each entry in the store that matches the filter.
    // it returns an error if the operation failed.
    List(ctx context.Context, filter string, handler EntryHandler) error

    // Apply applies the mutator to the entry with the given key. 
    // If the entry doesn't exist, it will be created. If during the mutation the entry is modified by another 
    // process, the mutator will be reprocessed with the new entry. If the mutator returns nil, the entry will be 
    // deleted.
    Apply(ctx context.Context, key string, mutator Mutator) (uint64, error)

    // Exists returns true if the entry with the given key exists in the store.
    Exists(ctx context.Context, key string) (bool, error)

    // Get returns the entry with the given key. If the entry doesn't exist, the second return value is false.
    Get(ctx context.Context, key string) (*Entry, bool, error)

    // Delete puts a delete marker on the given key. It will not be deleted but instead marked for deletion and being
    // ignored by the List method.
    Delete(ctx context.Context, key string) error

    // Purge deletes the key from the underlying stream. After doing a purge, the key will truly be deleted.
    Purge(ctx context.Context, key string) error
}
