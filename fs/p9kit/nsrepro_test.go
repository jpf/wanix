package p9kit

import (
	"context"
	"testing"

	"tractor.dev/wanix/fs"
	"tractor.dev/wanix/fs/fskit"
	"tractor.dev/wanix/fs/vfs"
)

// TestNSLsMultiplier reproduces the wanix `ls` storm through the real
// vfs.NS + p9kit client path (the layers a gojs task's ls actually
// drives via api/readdir.go + api/stat.go), and prints the per-message
// wire histogram. Linux v9fs does `ls 3ds` in ~2 stats; wanix does
// ~255 over the same wire, so the multiplier lives in this Go path.
//
// The backend mirrors the user's device: a "3ds" directory of 10
// subdirectories, each holding a few files (so a full-tree walk is
// visibly more expensive than a single-level listing).
func TestNSLsMultiplier(t *testing.T) {
	backend := fskit.MapFS{}
	subs := []string{"audio", "camera", "display", "input", "ir", "led", "nfc", "power", "sd", "system"}
	for _, s := range subs {
		backend["3ds/"+s+"/a"] = fskit.RawNode([]byte("x"))
		backend["3ds/"+s+"/b"] = fskit.RawNode([]byte("x"))
		backend["3ds/"+s+"/c"] = fskit.RawNode([]byte("x"))
	}

	client, cc, cleanup := countingSetup(t, backend)
	defer cleanup()

	ns := vfs.New(context.Background())
	if err := ns.Bind(client, ".", ".", vfs.BindReplace); err != nil {
		t.Fatalf("bind: %v", err)
	}
	cc.take() // discard setup + bind traffic

	dump := func(label string) map[uint8]int {
		got := cc.take()
		names := map[uint8]string{
			msgTlopen: "Tlopen", msgTgetattr: "Tgetattr", msgTreaddir: "Treaddir",
			msgTfsync: "Tfsync", msgTwalk: "Twalk", msgTclunk: "Tclunk",
		}
		total := 0
		line := ""
		for typ, n := range got {
			total += n
			nm := names[typ]
			if nm == "" {
				nm = "type" + string(rune('0'+typ))
			}
			line += " " + nm + "=" + itoa(n)
		}
		t.Logf("%-24s total=%d {%s }", label, total, line)
		return got
	}

	// Exactly what api/readdir.go does for `ls 3ds`. Reading a
	// directory through the namespace must NOT stat every entry: the
	// merge keeps DirEntries lazy (vfs.go), so a k-entry listing costs
	// one client listing, not k getattrs. Regression: this was 11
	// getattrs (dir + 10 entries) before the fix.
	entries, err := fs.ReadDir(ns, "3ds")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	rd := dump("readDir(3ds)")
	if len(entries) != 10 {
		t.Fatalf("entries = %d, want 10", len(entries))
	}
	if rd[msgTgetattr] > 1 {
		t.Errorf("readDir issued %d getattrs — the namespace is statting every entry again", rd[msgTgetattr])
	}

	// Exactly what filepath.Walk / ls then does: lstat every entry.
	// Each lstat must cost ONE upstream stat (walk+getattr+clunk), not
	// two: bind.Route no longer re-stats a single-candidate path that
	// the caller is about to stat anyway. Regression: this was 20
	// getattrs (2 per entry) before the fix.
	for _, e := range entries {
		if _, err := fs.Lstat(ns, "3ds/"+e.Name()); err != nil {
			t.Fatalf("Lstat %q: %v", e.Name(), err)
		}
	}
	ls := dump("10x Lstat(entry)")
	if ls[msgTgetattr] != 10 {
		t.Errorf("10 lstats issued %d getattrs, want 10 (one per entry)", ls[msgTgetattr])
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// TestNSLsRealistic mirrors a real wanix task namespace: a ramfs at
// ".", sibling roots, and the device mounted at "3ds" — then measures
// one ls. If this inflates well past the single-mount cost, the
// remaining multiplier is union/binding overhead, not the mount.
func TestNSLsRealistic(t *testing.T) {
	backend := fskit.MapFS{}
	subs := []string{"audio", "camera", "display", "input", "ir", "led", "nfc", "power", "sd", "system"}
	for _, s := range subs {
		for _, f := range []string{"a", "b", "c"} {
			backend["3ds/"+s+"/"+f] = fskit.RawNode([]byte("x"))
		}
	}
	client, cc, cleanup := countingSetup(t, backend)
	defer cleanup()

	ns := vfs.New(context.Background())
	// ramfs-like root union plus sibling roots, like repl-rc.
	ns.Bind(fskit.MapFS{"placeholder": fskit.RawNode([]byte(""))}, ".", ".", vfs.BindAfter)
	ns.Bind(fskit.MapFS{"x": fskit.RawNode([]byte(""))}, ".", "task", vfs.BindAfter)
	ns.Bind(fskit.MapFS{"x": fskit.RawNode([]byte(""))}, ".", "web", vfs.BindAfter)
	// the device mount
	ns.Bind(client, "3ds", "3ds", vfs.BindAfter)
	cc.take()

	total := func(m map[uint8]int) int {
		n := 0
		for _, v := range m {
			n += v
		}
		return n
	}
	entries, err := fs.ReadDir(ns, "3ds")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	rd := cc.take()
	for _, e := range entries {
		if _, err := fs.Lstat(ns, "3ds/"+e.Name()); err != nil {
			t.Fatalf("Lstat %q: %v", e.Name(), err)
		}
	}
	ls := cc.take()
	t.Logf("realistic ns: readDir stats=%d (total msgs=%d); 10x lstat stats=%d (total msgs=%d)",
		rd[msgTgetattr], total(rd), ls[msgTgetattr], total(ls))
}
