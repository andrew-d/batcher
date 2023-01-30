// Package batcher helpers to collect items from concurrent callers until
// either a certain time has elapsed or a configured batch size is reached, and
// then return that batch to be processed.
package batcher
