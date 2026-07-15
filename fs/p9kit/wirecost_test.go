package p9kit

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"

	"github.com/hugelgupf/p9/p9"
	"tractor.dev/wanix/fs"
	"tractor.dev/wanix/fs/fskit"
)

// 9P2000.L T-message types counted by the wire-cost baselines.
const (
	msgTlopen   = 12
	msgTgetattr = 24
	msgTreaddir = 40
	msgTfsync   = 50
	msgTwalk    = 110
	msgTclunk   = 120
)

// countingConn tallies the 9P T-messages the client writes. The p9
// client emits one message across several Write calls, so frames are
// reassembled from the byte stream by their size[4] header before the
// type byte at offset 4 is counted.
type countingConn struct {
	net.Conn
	mu     sync.Mutex
	buf    []byte
	counts map[uint8]int
}

func (cc *countingConn) Write(p []byte) (int, error) {
	cc.mu.Lock()
	cc.buf = append(cc.buf, p...)
	for len(cc.buf) >= 4 {
		size := binary.LittleEndian.Uint32(cc.buf)
		if size < 7 || uint32(len(cc.buf)) < size {
			break
		}
		cc.counts[cc.buf[4]]++
		cc.buf = cc.buf[size:]
	}
	cc.mu.Unlock()
	return cc.Conn.Write(p)
}

// take returns the counts accumulated since the last take and resets.
func (cc *countingConn) take() map[uint8]int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	got := cc.counts
	cc.counts = map[uint8]int{}
	return got
}

func countingSetup(t *testing.T, backend fs.FS) (fs.FS, *countingConn, func()) {
	t.Helper()
	a, b := net.Pipe()
	srv := p9.NewServer(Attacher(backend))
	done := make(chan error, 1)
	go func() {
		done <- srv.Handle(a, a)
	}()
	cc := &countingConn{Conn: b, counts: map[uint8]int{}}
	fsys, err := ClientFS(cc, "")
	if err != nil {
		t.Fatalf("ClientFS: %v", err)
	}
	cleanup := func() {
		b.Close()
		a.Close()
		<-done
	}
	return fsys, cc, cleanup
}

func expectCounts(t *testing.T, phase string, got map[uint8]int, want map[uint8]int) {
	t.Helper()
	names := map[uint8]string{
		msgTlopen: "Tlopen", msgTgetattr: "Tgetattr", msgTreaddir: "Treaddir",
		msgTfsync: "Tfsync", msgTwalk: "Twalk", msgTclunk: "Tclunk",
	}
	for typ, n := range want {
		if got[typ] != n {
			t.Errorf("%s: %s = %d, want %d", phase, names[typ], got[typ], n)
		}
	}
}

// TestLsWireCostBaseline pins the CURRENT wire cost of the operations
// the rc shell's `ls` drives through p9kit, so the pending listing
// fixes land as a measured reduction in these numbers rather than an
// unverified claim. Every assertion below documents pathological
// behavior on purpose:
//
//   - FS.ReadDir walks+getattrs+clunks EVERY entry individually
//     (client.go ReadDir), although the .L dirents it already holds
//     carry each entry's qid and type. For a k-entry directory that is
//     3k round-trips on top of the listing itself — the dominant term
//     in the 2026-07-15 "300 stats for one ls" relay trace.
//
//   - OpenContext tries ReadWrite first on everything (client.go
//     OpenContext); a directory always fails that and is retried
//     ReadOnly: two Tlopens and a server-side error per directory
//     open, visible as the deterministic Rerror in every listing
//     cycle of that trace.
//
//   - remoteFile.ReadDir issues one Tgetattr PER ENTRY on the
//     DIRECTORY'S OWN fid (client.go remoteFile.ReadDir), so every
//     entry also comes back wearing the directory's attributes — see
//     TestRemoteFileReadDirMislabelsEntries.
//
//   - remoteFile.Close fsyncs unconditionally, one wasted round-trip
//     per read-only handle.
//
// When the fixes land, the `want` maps below shrink; update them in
// the same commit so the diff records the before/after.
func TestLsWireCostBaseline(t *testing.T) {
	// Seven entries in the root: five files, two subdirectories.
	backend := fskit.MapFS{
		"f1": fskit.RawNode([]byte("x")), "f2": fskit.RawNode([]byte("x")),
		"f3": fskit.RawNode([]byte("x")), "f4": fskit.RawNode([]byte("x")),
		"f5":        fskit.RawNode([]byte("x")),
		"sub/inner": fskit.RawNode([]byte("x")),
		"sub2/deep": fskit.RawNode([]byte("x")),
	}
	fsys, cc, cleanup := countingSetup(t, backend)
	defer cleanup()
	cc.take() // discard version/attach setup traffic

	t.Run("FS.ReadDir of 7 entries", func(t *testing.T) {
		entries, err := fs.ReadDir(fsys, ".")
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		if len(entries) != 7 {
			t.Fatalf("entries = %d, want 7", len(entries))
		}
		expectCounts(t, "FS.ReadDir", cc.take(), map[uint8]int{
			msgTwalk:    9, // dir walk + clone + ONE PER ENTRY (pathological)
			msgTgetattr: 7, // ONE PER ENTRY (pathological)
			msgTclunk:   9, // per-entry fids + dir + clone
			msgTlopen:   1,
			msgTreaddir: 1,
		})
	})

	t.Run("OpenContext of a directory", func(t *testing.T) {
		f, err := fs.OpenContext(context.Background(), fsys, "sub")
		if err != nil {
			t.Fatalf("OpenContext: %v", err)
		}
		got := cc.take()
		expectCounts(t, "OpenContext(dir)", got, map[uint8]int{
			msgTlopen: 2, // ReadWrite attempt fails on a dir, retried ReadOnly (pathological)
			msgTwalk:  1,
		})

		// remoteFile.ReadDir: the os.File-shaped path a WASI/gojs
		// directory read lands on.
		rdf, ok := f.(fs.ReadDirFile)
		if !ok {
			t.Fatalf("not a ReadDirFile: %T", f)
		}
		if _, err := rdf.ReadDir(-1); err != nil {
			t.Fatalf("remoteFile.ReadDir: %v", err)
		}
		expectCounts(t, "remoteFile.ReadDir", cc.take(), map[uint8]int{
			msgTreaddir: 1,
			msgTgetattr: 1, // ONE PER ENTRY, on the directory's own fid (pathological)
			msgTwalk:    0,
		})

		if err := f.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		expectCounts(t, "Close", cc.take(), map[uint8]int{
			msgTfsync: 1, // unconditional fsync on a read-only handle (pathological)
			msgTclunk: 1,
		})
	})

	t.Run("ls-shaped composite", func(t *testing.T) {
		// The syscall sequence u-root ls's filepath.Walk drives for a
		// non-recursive listing: lstat the dir, list it, lstat every
		// entry. (The WASI shim's 1s readdir cache multiplies the
		// whole block over slow links — wasi/wanix.ts — which is not
		// reachable from Go; this pins the per-pass floor.)
		if _, err := fs.Stat(fsys, "."); err != nil {
			t.Fatal(err)
		}
		entries, err := fs.ReadDir(fsys, ".")
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if _, err := fs.Stat(fsys, e.Name()); err != nil {
				t.Fatal(err)
			}
		}
		got := cc.take()
		total := 0
		for _, n := range got {
			total += n
		}
		// Seven entries currently cost 51 messages for ONE listing pass:
		// Stat(dir)=3 + ReadDir=27 (walk+clone+open+list + 3 per entry)
		// + 7×Stat(entry)=21. Post-fix this should approach ~13
		// (walk+open+list+clunks + one lazy stat per entry at most).
		if total != 51 {
			t.Errorf("composite ls total = %d messages, want 51 (the pinned pathological baseline)", total)
		}
		expectCounts(t, "composite", got, map[uint8]int{
			msgTgetattr: 15, // dir + 7 in ReadDir + 7 in per-entry Stat
			msgTwalk:    17,
			msgTclunk:   17,
		})
	})
}

// TestRemoteFileReadDirMislabelsEntries documents (does not endorse) a
// correctness bug the wire-cost fix must also address: remoteFile.
// ReadDir stats the DIRECTORY's own fid for every entry, so each entry
// reports the directory's attributes under its own name — here a plain
// file claims to be a directory.
func TestRemoteFileReadDirMislabelsEntries(t *testing.T) {
	backend := fskit.MapFS{"sub/inner": fskit.RawNode([]byte("hello"))}
	fsys, _, cleanup := countingSetup(t, backend)
	defer cleanup()

	f, err := fs.OpenContext(context.Background(), fsys, "sub")
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	defer f.Close()
	entries, err := f.(fs.ReadDirFile).ReadDir(-1)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "inner" {
		t.Fatalf("entries = %v", entries)
	}
	// "inner" is a 5-byte regular file; today it wears sub's attrs.
	if !entries[0].IsDir() {
		t.Fatalf("baseline shifted: entry no longer mislabeled as a dir — " +
			"if this is the fix landing, fold this test into the fixed expectations")
	}
}
