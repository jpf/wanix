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

// TestLsWireCostBaseline pins the wire cost of the operations the rc
// shell's `ls` drives through p9kit. The previous revision of this
// test pinned the pathological pre-fix numbers (the "300 stats for
// one ls" relay trace of 2026-07-15); the `want` maps now hold the
// fixed costs so a regression toward per-entry round-trips fails
// loudly. What changed, per phase:
//
//   - FS.ReadDir used to walk+getattr+clunk EVERY entry (3k extra
//     round-trips for k entries) although the .L dirents it already
//     held carry each entry's qid and type. It now builds lazyEntry
//     values straight from the dirents: 27 messages for 7 entries
//     became 4, attributes fetched only if Info() is called.
//
//   - OpenContext tried ReadWrite first on everything; a directory
//     always failed that and was retried ReadOnly — two Tlopens and a
//     deterministic Rerror per directory open. The walk qid now types
//     the target, and directories open ReadOnly directly.
//
//   - remoteFile.ReadDir issued one Tgetattr PER ENTRY on the
//     DIRECTORY'S OWN fid, so every entry came back wearing the
//     directory's attributes (a plain file claimed IsDir()==true —
//     see TestRemoteFileReadDirLabelsEntries). Entries are now typed
//     by their dirent qid; a lazy Info() walks to the entry itself.
//
//   - remoteFile.Close fsynced unconditionally; it now fsyncs only
//     handles that were opened writable.
//
// Composite effect: one ls-shaped pass over 7 entries fell from 51
// messages to 28 (and the remaining floor is the caller's per-entry
// Stat, not ReadDir overhead).
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
			msgTwalk:    1, // just the directory itself — entries ride the dirents
			msgTgetattr: 0,
			msgTclunk:   1,
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
			msgTlopen: 1, // walk qid says dir → straight to ReadOnly
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
			msgTgetattr: 0, // entry types come from the dirent qids
			msgTwalk:    0,
		})

		if err := f.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		expectCounts(t, "Close", cc.take(), map[uint8]int{
			msgTfsync: 0, // read-only handles are not fsynced
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
		// Seven entries cost 28 messages per listing pass, down from the
		// 51 this test pinned pre-fix: Stat(dir)=3 + ReadDir=4
		// (walk+open+list+clunk) + 7×Stat(entry)=21. The per-entry
		// Stats are the caller's own (filepath.Walk lstats everything);
		// ReadDir itself no longer adds per-entry traffic.
		if total != 28 {
			t.Errorf("composite ls total = %d messages, want 28 (was 51 before the listing fixes)", total)
		}
		expectCounts(t, "composite", got, map[uint8]int{
			msgTgetattr: 8, // dir + 7 in per-entry Stat; none from ReadDir
			msgTwalk:    9,
			msgTclunk:   9,
		})
	})
}

// TestRemoteFileReadDirLabelsEntries covers the correctness half of
// the listing fix: remoteFile.ReadDir used to stat the DIRECTORY's own
// fid for every entry, so each entry reported the directory's
// attributes — a plain file claimed IsDir()==true. Entries are now
// typed by their own dirent qid, and Info() walks to the entry itself.
func TestRemoteFileReadDirLabelsEntries(t *testing.T) {
	backend := fskit.MapFS{"sub/inner": fskit.RawNode([]byte("hello"))}
	fsys, cc, cleanup := countingSetup(t, backend)
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
	if entries[0].IsDir() {
		t.Fatalf("inner is a plain file but IsDir() = true (the pre-fix mislabel)")
	}
	if entries[0].Type() != 0 {
		t.Fatalf("Type() = %v, want 0 (regular file)", entries[0].Type())
	}

	// Info() is lazy: it walks to the entry itself and stats it there,
	// on demand, rather than mislabeling it up front.
	cc.take()
	fi, err := entries[0].Info()
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if fi.IsDir() || fi.Size() != 5 {
		t.Fatalf("Info = dir:%v size:%d, want plain 5-byte file", fi.IsDir(), fi.Size())
	}
	expectCounts(t, "lazy Info", cc.take(), map[uint8]int{
		msgTwalk:    1,
		msgTgetattr: 1,
		msgTclunk:   1,
	})
}
